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
    private readonly InboxOutcomeWriter _outcomes;
    private readonly ILogger<MongoBatchMessageDispatcher> _log;
    private readonly IClaimCheckManager _claimCheck;
    private readonly IReadOnlyDictionary<(string EndpointId, string TypeId), BatchDispatchRegistration> _dispatchMap;
    private readonly IReadOnlyDictionary<string, int> _maxAttemptsMap;
    private readonly IReadOnlyDictionary<(string EndpointId, string TypeId), IConsumerDefinition> _definitionMap;
    private readonly IReadOnlyList<IBatchObserver> _observers;

    public MongoBatchMessageDispatcher(
        IServiceProvider sp,
        ICloudEventSerializer serializer,
        IMongoDatabase db,
        ILogger<MongoBatchMessageDispatcher> log,
        IClaimCheckManager claimCheck,
        IEnumerable<IConsumerDefinition> definitions,
        IEnumerable<IBatchObserver> observers)
    {
        _sp = sp;
        _serializer = serializer;
        _outcomes = new InboxOutcomeWriter(db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName), log);
        _log = log;
        _claimCheck = claimCheck;
        _observers = observers.ToList();

        var batchDefinitions = definitions.OfType<IBatchConsumerDefinition>().ToList();
        _dispatchMap = DispatchRegistrationBuilder.BuildBatchDispatchMap(batchDefinitions);
        _maxAttemptsMap = batchDefinitions
            .GroupBy(d => d.EndpointName)
            .ToDictionary(g => g.Key, g => g.Max(d => d.MaxAttempts));
        _definitionMap = batchDefinitions.ToDictionary(d => (d.EndpointName, d.TypeId), d => (IConsumerDefinition)d);
    }

    public async Task DispatchBatchAsync(IReadOnlyList<InboxMessage> messages, BatchConsumeContext context, CancellationToken ct)
    {
        if (messages.Count == 0)
            return;

        using var scope = _sp.CreateScope();
        BatchDispatchRegistration? reg = null;
        IReadOnlyList<InboxMessage> readableMessages = messages;
        try
        {
            reg = GetRegistration(context.EndpointId, context.TypeId);

            var batchItems = await ResolveReadableItemsAsync(messages, context, reg, ct);
            readableMessages = batchItems.Select(item => item.Message).ToList();
            if (readableMessages.Count == 0)
                return;

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

                NotifyBatchProcessed(new BatchMetrics(
                    groupContext.EndpointId,
                    groupContext.TypeId,
                    payloads.Count,
                    groupContext.BatchCompletedUtc - groupContext.BatchStartedUtc,
                    groupContext.GroupKey,
                    reg.FlushMode));
            }

            await MarkProcessedAsync(readableMessages);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _log.LogInformation("Stopped while handling a batch of {Count} messages on endpoint {EndpointId}; releasing them for redelivery.", readableMessages.Count, context.EndpointId);
            await ReleaseLocksAsync(readableMessages);
        }
        catch (Exception ex)
        {
            if (reg != null)
            {
                NotifyBatchFailed(new BatchFailureMetrics(
                    context.EndpointId,
                    context.TypeId,
                    readableMessages.Count,
                    DateTime.UtcNow - context.BatchStartedUtc,
                    reg.FailureMode,
                    ex));
            }
            await HandleDispatchFailureAsync(readableMessages, context, ex);
        }
    }

    private BatchDispatchRegistration GetRegistration(string endpointId, string typeId)
    {
        if (!_dispatchMap.TryGetValue((endpointId, typeId), out var reg))
            throw new InvalidOperationException($"No batch consumer registered for type '{typeId}' on endpoint '{endpointId}'.");

        return reg;
    }

    /// <summary>
    /// Reads each message's payload. A message that cannot be read, such as one with malformed JSON or a
    /// missing claim-check object, is recorded as failed on its own and left out of the batch, so it does
    /// not fail the messages it was batched with.
    /// </summary>
    private async Task<IReadOnlyList<BatchItem>> ResolveReadableItemsAsync(
        IReadOnlyList<InboxMessage> messages,
        BatchConsumeContext context,
        BatchDispatchRegistration reg,
        CancellationToken ct)
    {
        var items = new List<BatchItem>(messages.Count);
        for (var i = 0; i < messages.Count; i++)
        {
            var msg = messages[i];
            try
            {
                var payload = await ResolvePayloadAsync(msg, reg.MessageClrType, ct);
                items.Add(new BatchItem(msg, context.Messages[i], payload));
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _log.LogError(ex, "Could not read message {MessageId} on endpoint {EndpointId}; recording it as failed and handling the rest of the batch without it.", msg.Id, msg.EndpointId);
                await RecordFailureAsync([msg], reg, ex);
            }
        }

        return items;
    }

    private async Task<object> ResolvePayloadAsync(InboxMessage msg, Type messageType, CancellationToken ct)
    {
        using var doc = _serializer.Parse(msg.PayloadJson);
        var (dataEl, dataContentType) = GetDataEnvelope(doc.RootElement);
        return await ResolveDataAsync(dataEl, dataContentType, messageType, ct);
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

    private Task MarkProcessedAsync(IReadOnlyList<InboxMessage> messages) =>
        _outcomes.RecordAsync(messages, Builders<InboxMessage>.Update
            .Set(x => x.Status, InboxStatus.Processed)
            .Set(x => x.ProcessedUtc, DateTime.UtcNow)
            .Set(x => x.LockOwner, null)
            .Set(x => x.LockedUntilUtc, null));

    private Task ReleaseLocksAsync(IReadOnlyList<InboxMessage> messages) =>
        _outcomes.RecordAsync(messages, Builders<InboxMessage>.Update
            .Set(x => x.LockOwner, null)
            .Set(x => x.LockedUntilUtc, null));

    private async Task HandleDispatchFailureAsync(IReadOnlyList<InboxMessage> messages, BatchConsumeContext context, Exception ex)
    {
        _log.LogError(ex, "Error processing batch for endpoint {EndpointId} ({Count} messages)", context.EndpointId, messages.Count);

        await RecordFailureAsync(messages, GetRegistration(context.EndpointId, context.TypeId), ex);
    }

    private async Task RecordFailureAsync(IReadOnlyList<InboxMessage> messages, BatchDispatchRegistration reg, Exception ex)
    {
        if (reg.FailureMode == BatchFailureMode.MarkDead)
        {
            foreach (var msg in messages)
            {
                await _outcomes.RecordAsync([msg], Builders<InboxMessage>.Update
                    .Set(x => x.Status, InboxStatus.Dead)
                    .Set(x => x.Attempt, msg.Attempt + 1)
                    .Set(x => x.LastError, ErrorMessageFormatter.Describe(ex))
                    .Set(x => x.LockOwner, null)
                    .Set(x => x.LockedUntilUtc, null));
            }
            return;
        }

        foreach (var msg in messages)
        {
            await HandleRetryAsync(msg, ex);
        }
    }

    private async Task HandleRetryAsync(InboxMessage msg, Exception ex)
    {
        var nextAttempt = msg.Attempt + 1;
        var maxAttempts = _maxAttemptsMap.GetValueOrDefault(msg.EndpointId, 10);

        if (!ShouldRetry(msg, ex))
        {
            _log.LogWarning(
                "Message {MessageId} on endpoint {EndpointId} failed with a non-retryable {ExceptionType}. Moving to Dead.",
                msg.Id, msg.EndpointId, HandlerException.Unwrap(ex).GetType().Name);
            await DeadLetterAsync(msg, nextAttempt, ex);
            return;
        }

        if (nextAttempt >= maxAttempts)
        {
            _log.LogWarning("Message {MessageId} reached max attempts ({MaxAttempts}) on endpoint {EndpointId}. Moving to Dead.", msg.Id, maxAttempts, msg.EndpointId);
            await DeadLetterAsync(msg, nextAttempt, ex);
            return;
        }

        var delay = TimeSpan.FromSeconds(Math.Pow(2, nextAttempt));
        await _outcomes.RecordAsync([msg], Builders<InboxMessage>.Update
            .Set(x => x.Attempt, nextAttempt)
            .Set(x => x.VisibleUtc, DateTime.UtcNow.Add(delay))
            .Set(x => x.LastError, ErrorMessageFormatter.Describe(ex))
            .Set(x => x.Status, InboxStatus.Pending)
            .Set(x => x.LockOwner, null)
            .Set(x => x.LockedUntilUtc, null));
    }

    /// <summary>
    /// A consumer decides whether its own failure is worth retrying. The exception is unwrapped first, because a
    /// handler that throws synchronously arrives wrapped by the reflection call that invoked it.
    /// </summary>
    private bool ShouldRetry(InboxMessage msg, Exception ex) =>
        !_definitionMap.TryGetValue((msg.EndpointId, msg.TypeId), out var definition)
        || definition.ShouldRetry(HandlerException.Unwrap(ex));

    private Task DeadLetterAsync(InboxMessage msg, int attempt, Exception ex) =>
        _outcomes.RecordAsync([msg], Builders<InboxMessage>.Update
            .Set(x => x.Status, InboxStatus.Dead)
            .Set(x => x.Attempt, attempt)
            .Set(x => x.LastError, ErrorMessageFormatter.Describe(ex))
            .Set(x => x.LockOwner, null)
            .Set(x => x.LockedUntilUtc, null));

    private void NotifyBatchProcessed(BatchMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnBatchProcessed(metrics);
        }
    }

    private void NotifyBatchFailed(BatchFailureMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnBatchFailed(metrics);
        }
    }
}
