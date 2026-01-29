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
    IEnumerable<IPublishInterceptor> interceptors) : IMessageBus
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
    private readonly IMongoCollection<Binding> _bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);

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

        var finalSource = source ?? options.DefaultSource ?? "urn:mongobus:unknown";
        var context = BusContext.Current;
        var finalCorrelationId = correlationId ?? context?.CorrelationId ?? Guid.NewGuid().ToString("N");
        var finalCausationId = causationId ?? context?.MessageId.ToString();

        var publishContext = new PublishContext<T>(
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

        var interceptorList = interceptors.ToList();
        
        async Task CorePublishAsync()
        {
            var topic = publishContext.TypeId;
            var routes = await _bindings.Find(x => x.Topic == topic).ToListAsync(ct);
            
            if (routes.Count == 0) return;

            var claimCheckDecision = await claimCheck.TryStoreAsync(publishContext, ct);

            string payload;
            string cloudEventId;
            if (claimCheckDecision.IsClaimCheck)
            {
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
                payload = serializer.Serialize(claimEnvelope);
                cloudEventId = claimEnvelope.Id;
            }
            else
            {
                var envelope = enveloper.CreateEnvelope(publishContext);
                payload = serializer.Serialize(envelope);
                cloudEventId = envelope.Id;
            }

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
                Status = "Pending",
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
        }

        try
        {
            if (interceptorList.Count == 0)
            {
                await CorePublishAsync();
            }
            else
            {
                int index = 0;
                async Task Next()
                {
                    if (index < interceptorList.Count)
                    {
                        var interceptor = interceptorList[index++];
                        await interceptor.OnPublishAsync(publishContext, Next, ct);
                    }
                    else
                    {
                        await CorePublishAsync();
                    }
                }

                await Next();
            }
        }
        catch (Exception ex)
        {
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
}
