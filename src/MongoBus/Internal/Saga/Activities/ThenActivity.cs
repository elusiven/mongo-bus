using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

internal sealed class ThenActivity<TInstance, TMessage>(
    Action<SagaConsumeContext<TInstance, TMessage>> action)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        action(context);
        return Task.CompletedTask;
    }
}

internal sealed class ThenAsyncActivity<TInstance, TMessage>(
    Func<SagaConsumeContext<TInstance, TMessage>, Task> action)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        return action(context);
    }
}
