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
    /// cancellation token is cancelled if the lock is lost. Applies to every single-message consumer on the same
    /// endpoint; batch consumers do not renew. Requires a <see cref="LockTime"/> of at least one second.
    /// </summary>
    bool RenewLock => false;

    /// <summary>
    /// Whether a failure of this consumer is worth retrying. Returning false dead-letters the message on its first
    /// failure instead of retrying it up to <see cref="MaxAttempts"/>. The exception is the one the handler threw,
    /// with any reflection wrapper already removed.
    /// </summary>
    bool ShouldRetry(Exception exception) => true;
}
