using MongoBus.Abstractions;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

/// <summary>
/// Buffers <see cref="IMessageBus.PublishAsync{T}"/> calls made while a saga behaviour runs, so they are sent
/// only once the saga's new state has been written. A behaviour whose state write fails, for example on a
/// version conflict with another event for the same saga, publishes nothing.
///
/// Each buffered call captures its generic <c>T</c> in closures. Without the outbox, the calls are replayed
/// through <see cref="IMessageBus"/> after the state write. With the outbox, they are replayed through
/// <see cref="ITransactionalMessageBus.PublishToOutboxAsync"/> inside the transaction that writes the state,
/// so if the transaction aborts no outbox rows survive.
/// </summary>
internal sealed class BufferedSagaMessageBus : IMessageBus
{
    private readonly List<BufferedPublish> _deferred = new();

    public int BufferedCount => _deferred.Count;

    public Task PublishAsync<T>(
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
        _deferred.Add(new BufferedPublish(
            (bus, flushCt) => bus.PublishAsync(
                typeId,
                data,
                source,
                subject,
                id,
                timeUtc,
                deliverAt,
                correlationId,
                causationId,
                useClaimCheck,
                flushCt),
            (outbox, session, flushCt) => outbox.PublishToOutboxAsync(
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
                flushCt)));
        return Task.CompletedTask;
    }

    public async Task FlushAsync(IMessageBus bus, CancellationToken ct)
    {
        foreach (var publish in _deferred)
            await publish.ToBus(bus, ct);
    }

    public async Task FlushAsync(
        ITransactionalMessageBus outbox,
        IClientSessionHandle session,
        CancellationToken ct)
    {
        foreach (var publish in _deferred)
            await publish.ToOutbox(outbox, session, ct);
    }

    private sealed record BufferedPublish(
        Func<IMessageBus, CancellationToken, Task> ToBus,
        Func<ITransactionalMessageBus, IClientSessionHandle, CancellationToken, Task> ToOutbox);
}
