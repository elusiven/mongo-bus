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

    public MongoMessageDispatcher(
        IServiceProvider sp,
        ICloudEventSerializer serializer,
        IMongoDatabase db,
        ILogger<MongoMessageDispatcher> log,
        IClaimCheckManager claimCheck,
        IEnumerable<IConsumerDefinition> definitions)
    {
        _sp = sp;
        _serializer = serializer;
        _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        _log = log;
        _claimCheck = claimCheck;
        _dispatchMap = BuildDispatchMap(definitions);
        _maxAttemptsMap = definitions
            .GroupBy(d => d.EndpointName)
            .ToDictionary(g => g.Key, g => g.Max(d => d.MaxAttempts));
    }

    public async Task DispatchAsync(InboxMessage msg, ConsumeContext context, CancellationToken ct)
    {
        using var scope = _sp.CreateScope();
        try
        {
            if (!_dispatchMap.TryGetValue((msg.EndpointId, msg.TypeId), out var reg))
                throw new InvalidOperationException($"No consumer registered for type '{msg.TypeId}' on endpoint '{msg.EndpointId}'.");

            // Extract CloudEvent metadata from JSON manually (as per existing logic, could be improved)
            using var doc = _serializer.Parse(msg.PayloadJson);
            var root = doc.RootElement;
            
            var traceParent = root.TryGetProperty("traceParent", out var tpEl) ? tpEl.GetString() : null;
            var traceState = root.TryGetProperty("traceState", out var tsEl) ? tsEl.GetString() : null;

            using var activity = MongoBusDiagnostics.ActivitySource.StartActivity(
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

            var dataEl = root.GetProperty("data");
            var dataContentType = root.TryGetProperty("dataContentType", out var dctEl) ? dctEl.GetString() : null;

            object dataObj;
            if (string.Equals(dataContentType, ClaimCheckConstants.ContentType, StringComparison.OrdinalIgnoreCase))
            {
                var reference = _serializer.Deserialize<ClaimCheckReference>(dataEl.GetRawText());
                dataObj = await _claimCheck.ResolveAsync(reference, reg.MessageClrType, ct);
            }
            else
            {
                dataObj = _serializer.Deserialize(dataEl.GetRawText(), reg.MessageClrType);
            }

            var consumeInterceptors = scope.ServiceProvider.GetServices<IConsumeInterceptor>().ToList();

            async Task InvokeHandlerAsync()
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

            if (consumeInterceptors.Count == 0)
            {
                await InvokeHandlerAsync();
            }
            else
            {
                int index = 0;
                async Task Next()
                {
                    if (index < consumeInterceptors.Count)
                    {
                        var interceptor = consumeInterceptors[index++];
                        await interceptor.OnConsumeAsync(context, dataObj, Next, ct);
                    }
                    else
                    {
                        await InvokeHandlerAsync();
                    }
                }
                await Next();
            }

            // Mark as processed
            await _inbox.UpdateOneAsync(
                x => x.Id == msg.Id,
                Builders<InboxMessage>.Update
                    .Set(x => x.Status, "Processed")
                    .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                    .Set(x => x.LockOwner, null)
                    .Set(x => x.LockedUntilUtc, null),
                cancellationToken: ct);
        }
        catch (Exception ex)
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
                        .Set(x => x.Status, "Dead")
                        .Set(x => x.Attempt, nextAttempt)
                        .Set(x => x.LastError, ex.ToString())
                        .Set(x => x.LockOwner, null)
                        .Set(x => x.LockedUntilUtc, null),
                    cancellationToken: ct);
            }
            else
            {
                var delay = TimeSpan.FromSeconds(Math.Pow(2, nextAttempt));
                await _inbox.UpdateOneAsync(
                    x => x.Id == msg.Id,
                    Builders<InboxMessage>.Update
                        .Set(x => x.Attempt, nextAttempt)
                        .Set(x => x.VisibleUtc, DateTime.UtcNow.Add(delay))
                        .Set(x => x.LastError, ex.Message)
                        .Set(x => x.Status, "Pending")
                        .Set(x => x.LockOwner, null)
                        .Set(x => x.LockedUntilUtc, null),
                    cancellationToken: ct);
            }
        }
    }

    private static IReadOnlyDictionary<(string EndpointId, string TypeId), DispatchRegistration> BuildDispatchMap(IEnumerable<IConsumerDefinition> definitions)
    {
        var dispatch = new Dictionary<(string EndpointId, string TypeId), DispatchRegistration>();
        foreach (var def in definitions)
        {
            var key = (def.EndpointName, def.TypeId);
            var handlerInterface = typeof(IMessageHandler<>).MakeGenericType(def.MessageType);
            var handlerType = def.ConsumerType;

            if (dispatch.ContainsKey(key))
                throw new InvalidOperationException($"Duplicate (endpoint,type) registration: {def.EndpointName} / {def.TypeId}");

            var method = handlerInterface.GetMethod(nameof(IMessageHandler<object>.HandleAsync))!;
            Task HandlerDelegate(object handler, object data, ConsumeContext ctx, CancellationToken ct) =>
                (Task)method.Invoke(handler, [data, ctx, ct])!;

            dispatch[key] = new DispatchRegistration(def.EndpointName, def.TypeId, def.MessageType, handlerType, HandlerDelegate);
        }
        return dispatch;
    }
}
