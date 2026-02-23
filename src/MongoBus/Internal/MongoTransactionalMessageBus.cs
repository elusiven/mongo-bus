using System.Diagnostics;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MongoTransactionalMessageBus(
    IMongoClient client,
    IMongoDatabase db,
    MongoBusOptions options,
    ICloudEventEnveloper enveloper,
    ICloudEventSerializer serializer,
    IClaimCheckManager claimCheck,
    IEnumerable<IPublishInterceptor> interceptors,
    IEnumerable<IPublishObserver> observers) : ITransactionalMessageBus
{
    private readonly IMongoCollection<OutboxMessage> _outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
    private readonly IMongoCollection<Binding> _bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
    private readonly IReadOnlyList<IPublishObserver> _observers = observers.ToList();

    public async Task PublishToOutboxAsync<T>(
        string typeId,
        T data,
        IClientSessionHandle? session = null,
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
        if (!options.Outbox.Enabled)
            throw new InvalidOperationException("Outbox is disabled. Enable MongoBusOptions.Outbox.Enabled to use transactional publishing.");

        using var activity = MongoBusDiagnostics.ActivitySource.StartActivity($"{typeId} publish.outbox", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "mongodb");
        activity?.SetTag("messaging.destination.name", typeId);
        activity?.SetTag("messaging.operation", "publish");
        activity?.SetTag("messaging.mongodb.outbox", true);

        var publishContext = BuildPublishContext(typeId, data, source, subject, id, timeUtc, deliverAt, correlationId, causationId, useClaimCheck);
        var interceptorList = interceptors.ToList();
        var sw = Stopwatch.StartNew();
        var endpointCount = 0;

        try
        {
            endpointCount = (int)await _bindings.CountDocumentsAsync(x => x.Topic == typeId, cancellationToken: ct);
            if (interceptorList.Count == 0)
            {
                await CorePublishToOutboxAsync(session, publishContext, ct);
            }
            else
            {
                await InvokeWithInterceptorsAsync(interceptorList, publishContext, async () =>
                {
                    await CorePublishToOutboxAsync(session, publishContext, ct);
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

    public async Task PublishWithTransactionAsync<T>(
        string typeId,
        T data,
        Func<IClientSessionHandle, CancellationToken, Task> transactionCallback,
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
        ArgumentNullException.ThrowIfNull(transactionCallback);

        using var session = await client.StartSessionAsync(cancellationToken: ct);
        session.StartTransaction();
        try
        {
            await transactionCallback(session, ct);

            await PublishToOutboxAsync(
                typeId,
                data,
                session,
                source,
                subject,
                id,
                timeUtc,
                deliverAt,
                correlationId,
                causationId,
                useClaimCheck,
                ct);

            await session.CommitTransactionAsync(ct);
        }
        catch
        {
            await session.AbortTransactionAsync(ct);
            throw;
        }
    }

    private async Task CorePublishToOutboxAsync<T>(IClientSessionHandle? session, PublishContext<T> publishContext, CancellationToken ct)
    {
        var topic = publishContext.TypeId;
        var (payload, cloudEventId) = await BuildPayloadAsync(publishContext, ct);
        var now = DateTime.UtcNow;

        var outboxMessage = new OutboxMessage
        {
            Topic = topic,
            TypeId = publishContext.TypeId,
            PayloadJson = payload,
            CreatedUtc = now,
            VisibleUtc = publishContext.DeliverAt ?? now,
            Attempt = 0,
            Status = OutboxStatus.Pending,
            CorrelationId = publishContext.CorrelationId,
            CausationId = publishContext.CausationId,
            CloudEventId = cloudEventId
        };

        if (session is null)
        {
            await _outbox.InsertOneAsync(outboxMessage, cancellationToken: ct);
        }
        else
        {
            await _outbox.InsertOneAsync(session, outboxMessage, cancellationToken: ct);
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
