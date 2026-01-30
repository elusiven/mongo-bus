using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MongoBatchMessageDispatcher : IBatchMessageDispatcher
{
    private readonly IServiceProvider _sp;
    private readonly ICloudEventSerializer _serializer;
    private readonly IMongoCollection<InboxMessage> _inbox;
    private readonly ILogger<MongoBatchMessageDispatcher> _log;
    private readonly IClaimCheckManager _claimCheck;
    private readonly IReadOnlyDictionary<(string EndpointId, string TypeId), BatchDispatchRegistration> _dispatchMap;
    private readonly IReadOnlyDictionary<string, int> _maxAttemptsMap;

    public MongoBatchMessageDispatcher(
        IServiceProvider sp,
        ICloudEventSerializer serializer,
        IMongoDatabase db,
        ILogger<MongoBatchMessageDispatcher> log,
        IClaimCheckManager claimCheck,
        IEnumerable<IConsumerDefinition> definitions)
    {
        _sp = sp;
        _serializer = serializer;
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _log = log;
        _claimCheck = claimCheck;

        var batchDefinitions = definitions.OfType<IBatchConsumerDefinition>().ToList();
        _dispatchMap = DispatchRegistrationBuilder.BuildBatchDispatchMap(batchDefinitions);
        _maxAttemptsMap = batchDefinitions
            .GroupBy(d => d.EndpointName)
            .ToDictionary(g => g.Key, g => g.Max(d => d.MaxAttempts));
    }

    public async Task DispatchBatchAsync(IReadOnlyList<InboxMessage> messages, BatchConsumeContext context, CancellationToken ct)
    {
        if (messages.Count == 0)
            return;

        using var scope = _sp.CreateScope();
        try
        {
            var reg = GetRegistration(context.EndpointId, context.TypeId);

            var batchItems = await ResolveBatchItemsAsync(messages, context, reg.MessageClrType, ct);

            var grouped = batchItems.GroupBy(item => reg.GroupingStrategy.GetGroupKey(item.Payload, item.Context));

            var consumeInterceptors = scope.ServiceProvider.GetServices<IBatchConsumeInterceptor>().ToList();
            foreach (var group in grouped)
            {
                var groupItems = group.ToList();
                var payloads = groupItems.Select(x => x.Payload).ToList();
                var ctxs = groupItems.Select(x => x.Context).ToList();
                var typedList = CreateTypedList(reg.MessageClrType, payloads);

                var groupContext = context with
                {
                    Messages = ctxs,
                    BatchCompletedUtc = DateTime.UtcNow,
                    GroupKey = group.Key
                };

                await InvokeWithInterceptorsAsync(
                    consumeInterceptors,
                    () => InvokeHandlerAsync(scope, reg, typedList, groupContext, ct),
                    groupContext,
                    payloads,
                    ct);
            }

            await MarkProcessedAsync(messages, ct);
        }
        catch (Exception ex)
        {
            await HandleDispatchFailureAsync(messages, context, ex, ct);
        }
    }

    private BatchDispatchRegistration GetRegistration(string endpointId, string typeId)
    {
        if (!_dispatchMap.TryGetValue((endpointId, typeId), out var reg))
            throw new InvalidOperationException($"No batch consumer registered for type '{typeId}' on endpoint '{endpointId}'.");

        return reg;
    }

    private async Task<IReadOnlyList<BatchItem>> ResolveBatchItemsAsync(
        IReadOnlyList<InboxMessage> messages,
        BatchConsumeContext context,
        Type messageType,
        CancellationToken ct)
    {
        var items = new List<BatchItem>(messages.Count);
        for (var i = 0; i < messages.Count; i++)
        {
            var msg = messages[i];
            using var doc = _serializer.Parse(msg.PayloadJson);
            var root = doc.RootElement;

            var (dataEl, dataContentType) = GetDataEnvelope(root);
            var dataObj = await ResolveDataAsync(dataEl, dataContentType, messageType, ct);
            var ctx = context.Messages[i];
            items.Add(new BatchItem(msg, ctx, dataObj));
        }

        return items;
    }

    private static (JsonElement DataElement, string? DataContentType) GetDataEnvelope(JsonElement root)
    {
        var dataEl = root.GetProperty("data");
        var dataContentType = root.TryGetProperty("dataContentType", out var dctEl) ? dctEl.GetString() : null;
        return (dataEl, dataContentType);
    }

    private async Task<object> ResolveDataAsync(JsonElement dataEl, string? dataContentType, Type messageType, CancellationToken ct)
    {
        if (string.Equals(dataContentType, ClaimCheckConstants.ContentType, StringComparison.OrdinalIgnoreCase))
        {
            var reference = _serializer.Deserialize<ClaimCheckReference>(dataEl.GetRawText());
            return await _claimCheck.ResolveAsync(reference, messageType, ct);
        }

        return _serializer.Deserialize(dataEl.GetRawText(), messageType);
    }

    private static object CreateTypedList(Type messageType, IReadOnlyList<object> payloads)
    {
        var listType = typeof(List<>).MakeGenericType(messageType);
        var list = (System.Collections.IList)Activator.CreateInstance(listType)!;
        foreach (var payload in payloads)
        {
            list.Add(payload);
        }
        return list;
    }

    private sealed record BatchItem(InboxMessage Message, ConsumeContext Context, object Payload);

    private static async Task InvokeHandlerAsync(
        IServiceScope scope,
        BatchDispatchRegistration reg,
        object dataList,
        BatchConsumeContext context,
        CancellationToken ct)
    {
        BusContext.Current = context.Messages.Count > 0 ? context.Messages[0] : null;
        try
        {
            var handler = scope.ServiceProvider.GetRequiredService(reg.HandlerInterface);
            await reg.HandlerDelegate(handler, dataList, context, ct);
        }
        catch (Exception ex)
        {
            if (MongoBusDiagnostics.ActivitySource.HasListeners())
            {
                using var activity = MongoBusDiagnostics.ActivitySource.StartActivity(
                    $"{context.TypeId} consume.batch",
                    ActivityKind.Consumer);
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddTag("exception.type", ex.GetType().FullName);
                activity?.AddTag("exception.message", ex.Message);
                activity?.AddTag("exception.stacktrace", ex.ToString());
            }
            throw;
        }
        finally
        {
            BusContext.Current = null;
        }
    }

    private static async Task InvokeWithInterceptorsAsync(
        IReadOnlyList<IBatchConsumeInterceptor> interceptors,
        Func<Task> handler,
        BatchConsumeContext context,
        IReadOnlyList<object> payloads,
        CancellationToken ct)
    {
        if (interceptors.Count == 0)
        {
            await handler();
            return;
        }

        var index = 0;
        async Task Next()
        {
            if (index < interceptors.Count)
            {
                var interceptor = interceptors[index++];
                await interceptor.OnConsumeBatchAsync(context, payloads, Next, ct);
            }
            else
            {
                await handler();
            }
        }

        await Next();
    }

    private Task MarkProcessedAsync(IReadOnlyList<InboxMessage> messages, CancellationToken ct)
    {
        var ids = messages.Select(m => m.Id).ToArray();
        return _inbox.UpdateManyAsync(
            x => ids.Contains(x.Id),
            Builders<InboxMessage>.Update
                .Set(x => x.Status, InboxStatus.Processed)
                .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null),
            cancellationToken: ct);
    }

    private async Task HandleDispatchFailureAsync(IReadOnlyList<InboxMessage> messages, BatchConsumeContext context, Exception ex, CancellationToken ct)
    {
        _log.LogError(ex, "Error processing batch for endpoint {EndpointId} ({Count} messages)", context.EndpointId, messages.Count);

        var reg = GetRegistration(context.EndpointId, context.TypeId);
        if (reg.FailureMode == BatchFailureMode.MarkDead)
        {
            foreach (var msg in messages)
            {
                await _inbox.UpdateOneAsync(
                    x => x.Id == msg.Id,
                    Builders<InboxMessage>.Update
                        .Set(x => x.Status, InboxStatus.Dead)
                        .Set(x => x.Attempt, msg.Attempt + 1)
                        .Set(x => x.LastError, ex.ToString())
                        .Set(x => x.LockOwner, null)
                        .Set(x => x.LockedUntilUtc, null),
                    cancellationToken: ct);
            }
            return;
        }

        foreach (var msg in messages)
        {
            await HandleRetryAsync(msg, ex, ct);
        }
    }

    private async Task HandleRetryAsync(InboxMessage msg, Exception ex, CancellationToken ct)
    {
        var nextAttempt = msg.Attempt + 1;
        var maxAttempts = _maxAttemptsMap.GetValueOrDefault(msg.EndpointId, 10);

        if (nextAttempt >= maxAttempts)
        {
            _log.LogWarning("Message {MessageId} reached max attempts ({MaxAttempts}) on endpoint {EndpointId}. Moving to Dead.", msg.Id, maxAttempts, msg.EndpointId);
            await _inbox.UpdateOneAsync(
                x => x.Id == msg.Id,
                Builders<InboxMessage>.Update
                    .Set(x => x.Status, InboxStatus.Dead)
                    .Set(x => x.Attempt, nextAttempt)
                    .Set(x => x.LastError, ex.ToString())
                    .Set(x => x.LockOwner, null)
                    .Set(x => x.LockedUntilUtc, null),
                cancellationToken: ct);
            return;
        }

        var delay = TimeSpan.FromSeconds(Math.Pow(2, nextAttempt));
        await _inbox.UpdateOneAsync(
            x => x.Id == msg.Id,
            Builders<InboxMessage>.Update
                .Set(x => x.Attempt, nextAttempt)
                .Set(x => x.VisibleUtc, DateTime.UtcNow.Add(delay))
                .Set(x => x.LastError, ex.Message)
                .Set(x => x.Status, InboxStatus.Pending)
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null),
            cancellationToken: ct);
    }
}
