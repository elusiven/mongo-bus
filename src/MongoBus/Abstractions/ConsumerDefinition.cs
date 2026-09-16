using MongoBus.Utils;

namespace MongoBus.Abstractions;

public abstract class ConsumerDefinition<TConsumer, TMessage> : IConsumerDefinition
    where TConsumer : class, IMessageHandler<TMessage>
{
    public abstract string TypeId { get; }
    public Type MessageType => typeof(TMessage);
    public Type ConsumerType => typeof(TConsumer);

    public virtual string EndpointName => EndpointNameHelper.FromConsumerType(typeof(TConsumer));
    public virtual int ConcurrencyLimit => 8;

    /// <summary>
    /// How many messages the pump may hold ready for its workers. A prefetched message is locked while it waits its
    /// turn, so prefetching more than the workers can take on lets those locks lapse and hands the messages to a
    /// competing consumer. Raise it above <see cref="ConcurrencyLimit"/> only for handlers fast enough to drain the
    /// extra messages well within <see cref="LockTime"/>.
    /// </summary>
    public virtual int PrefetchCount => ConcurrencyLimit;
    public virtual TimeSpan LockTime => TimeSpan.FromSeconds(60);
    public virtual int MaxAttempts => 10;
    public virtual bool IdempotencyEnabled => false;
    public virtual bool RenewLock => false;
}
