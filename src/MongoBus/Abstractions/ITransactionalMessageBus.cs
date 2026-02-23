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
