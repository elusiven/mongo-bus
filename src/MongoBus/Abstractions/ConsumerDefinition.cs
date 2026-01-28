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
    public virtual int PrefetchCount => 64;
    public virtual TimeSpan LockTime => TimeSpan.FromSeconds(60);
    public virtual int MaxAttempts => 10;
    public virtual bool IdempotencyEnabled => false;
}
