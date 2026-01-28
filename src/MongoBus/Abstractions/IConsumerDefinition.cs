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
}
