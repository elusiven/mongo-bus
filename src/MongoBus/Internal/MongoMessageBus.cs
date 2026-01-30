using System.Diagnostics;
using System.Text.Json;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal;

public sealed class MongoMessageBus(
    IMongoDatabase db, 
    MongoBusOptions options,
    ICloudEventEnveloper enveloper,
    ICloudEventSerializer serializer,
    IClaimCheckManager claimCheck,
    IEnumerable<IPublishInterceptor> interceptors,
    IEnumerable<IPublishObserver> observers) : IMessageBus
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
    private readonly IMongoCollection<Binding> _bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
    private readonly IReadOnlyList<IPublishObserver> _observers = observers.ToList();

    public async Task PublishAsync<T>(
        string typeId,
        T data,
        string? source = null,
        string? subject = null,
        string? id = null,
        DateTime? timeUtc = null,
        DateTime? deliverAt = null,
        string? correlationId = null,
        string? causationId = null,
        bool? useClaimCheck = null,
        CancellationToken ct = default)
    {
        using var activity = MongoBusDiagnostics.ActivitySource.StartActivity($"{typeId} publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "mongodb");
        activity?.SetTag("messaging.destination.name", typeId);
        activity?.SetTag("messaging.operation", "publish");

        var publishContext = BuildPublishContext(typeId, data, source, subject, id, timeUtc, deliverAt, correlationId, causationId, useClaimCheck);
        var interceptorList = interceptors.ToList();
        var sw = Stopwatch.StartNew();
        var endpointCount = 0;

        try
        {
            if (interceptorList.Count == 0)
            {
                endpointCount = await CorePublishAsync(publishContext, ct);
            }
            else
            {
                await InvokeWithInterceptorsAsync(interceptorList, publishContext, async () =>
                {
                    endpointCount = await CorePublishAsync(publishContext, ct);
                }, ct);
            }

            NotifyPublish(new PublishMetrics(typeId, endpointCount, sw.Elapsed));
        }
        catch (Exception ex)
        {
            NotifyPublishFailed(new PublishFailureMetrics(typeId, endpointCount, sw.Elapsed, ex));
            if (activity != null)
            {
                activity.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity.AddTag("exception.type", ex.GetType().FullName);
                activity.AddTag("exception.message", ex.Message);
                activity.AddTag("exception.stacktrace", ex.ToString());
            }
            throw;
        }
    }

    private PublishContext<T> BuildPublishContext<T>(
        string typeId,
        T data,
        string? source,
        string? subject,
        string? id,
        DateTime? timeUtc,
        DateTime? deliverAt,
        string? correlationId,
        string? causationId,
        bool? useClaimCheck)
    {
        var finalSource = source ?? options.DefaultSource ?? "urn:mongobus:unknown";
        var context = BusContext.Current;
        var finalCorrelationId = correlationId ?? context?.CorrelationId ?? Guid.NewGuid().ToString("N");
        var finalCausationId = causationId ?? context?.MessageId.ToString();

        return new PublishContext<T>(
            typeId,
            data,
            finalSource,
            subject,
            id,
            timeUtc,
            deliverAt,
            finalCorrelationId,
            finalCausationId)
        {
            UseClaimCheck = useClaimCheck
        };
    }

    private async Task<int> CorePublishAsync<T>(PublishContext<T> publishContext, CancellationToken ct)
    {
        var topic = publishContext.TypeId;
        var routes = await _bindings.Find(x => x.Topic == topic).ToListAsync(ct);

        if (routes.Count == 0) return 0;

        var (payload, cloudEventId) = await BuildPayloadAsync(publishContext, ct);
        var now = DateTime.UtcNow;

        var docs = routes.Select(r => new InboxMessage
        {
            EndpointId = r.EndpointId,
            Topic = topic,
            TypeId = publishContext.TypeId,
            PayloadJson = payload,
            CreatedUtc = now,
            VisibleUtc = publishContext.DeliverAt ?? now,
            Attempt = 0,
            Status = InboxStatus.Pending,
            CorrelationId = publishContext.CorrelationId,
            CausationId = publishContext.CausationId,
            CloudEventId = cloudEventId
        }).ToList();

        if (docs.Count == 1)
        {
            await _inbox.InsertOneAsync(docs[0], cancellationToken: ct);
        }
        else
        {
            await _inbox.InsertManyAsync(docs, cancellationToken: ct);
        }

        return docs.Count;
    }

    private async Task<(string Payload, string CloudEventId)> BuildPayloadAsync<T>(PublishContext<T> publishContext, CancellationToken ct)
    {
        var claimCheckDecision = await claimCheck.TryStoreAsync(publishContext, ct);
        if (!claimCheckDecision.IsClaimCheck)
        {
            var envelope = enveloper.CreateEnvelope(publishContext);
            return (serializer.Serialize(envelope), envelope.Id);
        }

        var claimContext = new PublishContext<ClaimCheckReference>(
            publishContext.TypeId,
            claimCheckDecision.Reference!,
            publishContext.Source,
            publishContext.Subject,
            publishContext.Id,
            publishContext.TimeUtc,
            publishContext.DeliverAt,
            publishContext.CorrelationId,
            publishContext.CausationId)
        {
            DataContentType = ClaimCheckConstants.ContentType,
            UseClaimCheck = publishContext.UseClaimCheck
        };

        var claimEnvelope = enveloper.CreateEnvelope(claimContext);
        return (serializer.Serialize(claimEnvelope), claimEnvelope.Id);
    }

    private static async Task InvokeWithInterceptorsAsync<T>(
        IReadOnlyList<IPublishInterceptor> interceptorList,
        PublishContext<T> context,
        Func<Task> handler,
        CancellationToken ct)
    {
        var index = 0;
        async Task Next()
        {
            if (index < interceptorList.Count)
            {
                var interceptor = interceptorList[index++];
                await interceptor.OnPublishAsync(context, Next, ct);
            }
            else
            {
                await handler();
            }
        }

        await Next();
    }

    private void NotifyPublish(PublishMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnPublish(metrics);
        }
    }

    private void NotifyPublishFailed(PublishFailureMetrics metrics)
    {
        if (_observers.Count == 0)
            return;

        foreach (var observer in _observers)
        {
            observer.OnPublishFailed(metrics);
        }
    }
}
