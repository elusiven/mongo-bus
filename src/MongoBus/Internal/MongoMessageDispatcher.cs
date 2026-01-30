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

internal sealed class MongoMessageDispatcher : IMessageDispatcher
{
    private readonly IServiceProvider _sp;
    private readonly ICloudEventSerializer _serializer;
    private readonly IMongoCollection<InboxMessage> _inbox;
    private readonly ILogger<MongoMessageDispatcher> _log;
    private readonly IClaimCheckManager _claimCheck;
    private readonly IReadOnlyDictionary<(string EndpointId, string TypeId), DispatchRegistration> _dispatchMap;
    private readonly IReadOnlyDictionary<string, int> _maxAttemptsMap;
    private readonly IReadOnlyList<IConsumeObserver> _observers;

    public MongoMessageDispatcher(
        IServiceProvider sp,
        ICloudEventSerializer serializer,
        IMongoDatabase db,
        ILogger<MongoMessageDispatcher> log,
        IClaimCheckManager claimCheck,
        IEnumerable<IConsumerDefinition> definitions,
        IEnumerable<IConsumeObserver> observers)
    {
        _sp = sp;
        _serializer = serializer;
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _log = log;
        _claimCheck = claimCheck;
        _observers = observers.ToList();
        var singleDefinitions = definitions.Where(d => d is not IBatchConsumerDefinition).ToList();
        _dispatchMap = DispatchRegistrationBuilder.BuildDispatchMap(singleDefinitions);
        _maxAttemptsMap = singleDefinitions
            .GroupBy(d => d.EndpointName)
            .ToDictionary(g => g.Key, g => g.Max(d => d.MaxAttempts));
    }

    public async Task DispatchAsync(InboxMessage msg, ConsumeContext context, CancellationToken ct)
    {
        using var scope = _sp.CreateScope();
        var sw = Stopwatch.StartNew();
        try
        {
            var reg = GetRegistration(msg);

            using var doc = _serializer.Parse(msg.PayloadJson);
            var root = doc.RootElement;

            var (traceParent, traceState) = ParseTraceContext(root);
            using var activity = StartConsumeActivity(msg, traceParent, traceState);

            var (dataEl, dataContentType) = GetDataEnvelope(root);
            var dataObj = await ResolveDataAsync(dataEl, dataContentType, reg.MessageClrType, ct);

            var consumeInterceptors = scope.ServiceProvider.GetServices<IConsumeInterceptor>().ToList();
            await InvokeWithInterceptorsAsync(consumeInterceptors, () => InvokeHandlerAsync(scope, reg, dataObj, context, activity, ct), context, dataObj, ct);

            await MarkProcessedAsync(msg, ct);

            NotifyMessageProcessed(new ConsumeMetrics(context, sw.Elapsed));
        }
        catch (Exception ex)
        {
            NotifyMessageFailed(new ConsumeFailureMetrics(context, sw.Elapsed, ex));
            await HandleDispatchFailureAsync(msg, ex, ct);
        }
    }

    private DispatchRegistration GetRegistration(InboxMessage msg)
    {
        if (!_dispatchMap.TryGetValue((msg.EndpointId, msg.TypeId), out var reg))
            throw new InvalidOperationException($"No consumer registered for type '{msg.TypeId}' on endpoint '{msg.EndpointId}'.");

        return reg;
    }

    private static (string? TraceParent, string? TraceState) ParseTraceContext(JsonElement root)
    {
        var traceParent = root.TryGetProperty("traceParent", out var tpEl) ? tpEl.GetString() : null;
        var traceState = root.TryGetProperty("traceState", out var tsEl) ? tsEl.GetString() : null;
        return (traceParent, traceState);
    }

    private static Activity? StartConsumeActivity(InboxMessage msg, string? traceParent, string? traceState)
    {
        var activity = MongoBusDiagnostics.ActivitySource.StartActivity(
            $"{msg.TypeId} consume",
            ActivityKind.Consumer,
            parentId: traceParent);

        if (activity != null && traceState != null)
        {
            activity.TraceStateString = traceState;
        }

        activity?.SetTag("messaging.system", "mongodb");
        activity?.SetTag("messaging.destination.name", msg.Topic);
        activity?.SetTag("messaging.operation", "process");
        activity?.SetTag("messaging.mongodb.endpoint", msg.EndpointId);

        return activity;
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

    private static async Task InvokeHandlerAsync(
        IServiceScope scope,
        DispatchRegistration reg,
        object dataObj,
        ConsumeContext context,
        Activity? activity,
        CancellationToken ct)
    {
        BusContext.Current = context;
        try
        {
            var handler = scope.ServiceProvider.GetRequiredService(reg.HandlerInterface);
            await reg.HandlerDelegate(handler, dataObj, context, ct);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            if (activity != null)
            {
                activity.AddTag("exception.type", ex.GetType().FullName);
                activity.AddTag("exception.message", ex.Message);
                activity.AddTag("exception.stacktrace", ex.ToString());
            }
            throw;
        }
        finally
        {
            BusContext.Current = null;
        }
    }

    private static async Task InvokeWithInterceptorsAsync(
        IReadOnlyList<IConsumeInterceptor> interceptors,
        Func<Task> handler,
        ConsumeContext context,
        object dataObj,
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
                await interceptor.OnConsumeAsync(context, dataObj, Next, ct);
            }
            else
            {
                await handler();
            }
        }

        await Next();
    }

    private Task MarkProcessedAsync(InboxMessage msg, CancellationToken ct) =>
        _inbox.UpdateOneAsync(
            x => x.Id == msg.Id,
            Builders<InboxMessage>.Update
                .Set(x => x.Status, InboxStatus.Processed)
                .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null),
            cancellationToken: ct);

    private async Task HandleDispatchFailureAsync(InboxMessage msg, Exception ex, CancellationToken ct)
    {
        _log.LogError(ex, "Error processing message {MessageId} on endpoint {EndpointId}", msg.Id, msg.EndpointId);

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

    private void NotifyMessageProcessed(ConsumeMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnMessageProcessed(metrics);
        }
    }

    private void NotifyMessageFailed(ConsumeFailureMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnMessageFailed(metrics);
        }
    }
}
