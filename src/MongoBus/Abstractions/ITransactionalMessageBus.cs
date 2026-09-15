using MongoDB.Driver;

namespace MongoBus.Abstractions;

public interface ITransactionalMessageBus
{
    Task PublishToOutboxAsync<T>(
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
        CancellationToken ct = default);

    /// <summary>
    /// Runs <paramref name="transactionCallback"/> and stages the message in the outbox inside one MongoDB transaction.
    /// Requires a replica set or sharded cluster.
    /// </summary>
    /// <remarks>
    /// The transaction is retried after transient errors such as a write conflict with a concurrent transaction, so
    /// <paramref name="transactionCallback"/> can run more than once. It must perform its writes through the session it
    /// is given and must not swallow exceptions from MongoDB; otherwise the driver cannot tell whether the transaction
    /// was aborted.
    /// </remarks>
    Task PublishWithTransactionAsync<T>(
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
        CancellationToken ct = default);
}
