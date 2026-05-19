using MongoBus.Abstractions;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

/// <summary>
/// Buffers <see cref="IMessageBus.PublishAsync{T}"/> calls during a saga activity chain so
/// they can be committed atomically with the saga state inside a MongoDB transaction.
///
/// Each buffered call captures its generic <c>T</c> via the lambda closure; <see
/// cref="FlushAsync"/> replays the captured arguments through
/// <see cref="ITransactionalMessageBus.PublishToOutboxAsync"/> using the supplied session.
/// If the transaction aborts, the buffer is dropped and no outbox rows survive.
/// </summary>
internal sealed class BufferedSagaMessageBus : IMessageBus
{
    private readonly List<Func<ITransactionalMessageBus, IClientSessionHandle, CancellationToken, Task>> _deferred = new();

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
        _deferred.Add((outbox, session, flushCt) => outbox.PublishToOutboxAsync(
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
            flushCt));
        return Task.CompletedTask;
    }

    public async Task FlushAsync(
        ITransactionalMessageBus outbox,
        IClientSessionHandle session,
        CancellationToken ct)
    {
        foreach (var publish in _deferred)
            await publish(outbox, session, ct);
    }
}
