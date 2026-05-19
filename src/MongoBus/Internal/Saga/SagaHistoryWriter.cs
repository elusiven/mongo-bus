using MongoBus.Abstractions.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

internal sealed class SagaHistoryWriter<TInstance>
    where TInstance : class, ISagaInstance
{
    private readonly IMongoCollection<SagaHistoryEntry> _collection;

    public SagaHistoryWriter(IMongoDatabase db)
    {
        var collectionName = $"bus_saga_history_{EndpointNameHelper.FromConsumerType(typeof(TInstance))}";
        _collection = db.GetCollection<SagaHistoryEntry>(collectionName);
    }

    public string CollectionName => _collection.CollectionNamespace.CollectionName;

    public Task WriteAsync(
        string correlationId,
        string previousState,
        string newState,
        string eventTypeId,
        ConsumeContext context,
        int versionAfter,
        CancellationToken ct)
        => WriteAsync(correlationId, previousState, newState, eventTypeId, context, versionAfter, session: null, ct);

    public async Task WriteAsync(
        string correlationId,
        string previousState,
        string newState,
        string eventTypeId,
        ConsumeContext context,
        int versionAfter,
        IClientSessionHandle? session,
        CancellationToken ct)
    {
        var entry = new SagaHistoryEntry
        {
            CorrelationId = correlationId,
            SagaType = typeof(TInstance).Name,
            PreviousState = previousState,
            NewState = newState,
            EventTypeId = eventTypeId,
            MessageId = context.CloudEventId,
            TimestampUtc = DateTime.UtcNow,
            VersionAfter = versionAfter
        };

        if (session is null)
            await _collection.InsertOneAsync(entry, cancellationToken: ct);
        else
            await _collection.InsertOneAsync(session, entry, cancellationToken: ct);
    }
}
