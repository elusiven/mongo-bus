namespace MongoBus.Abstractions;

public interface IConsumerDefinition
{
    string TypeId { get; }
    Type MessageType { get; }
    Type ConsumerType { get; }
    string EndpointName { get; }
    int ConcurrencyLimit { get; }
    int PrefetchCount { get; }
    TimeSpan LockTime { get; }
    int MaxAttempts { get; }
    bool IdempotencyEnabled { get; }

    /// <summary>
    /// When true, the lock on a message is extended for as long as its handler runs, and the handler's
    /// cancellation token is cancelled if the lock is lost. Applies to single-message consumers; batch
    /// consumers do not renew. Requires a <see cref="LockTime"/> of at least one second.
    /// </summary>
    bool RenewLock => false;
}
