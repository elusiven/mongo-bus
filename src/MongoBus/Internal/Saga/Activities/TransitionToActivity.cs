using MongoBus.Abstractions.Saga;
using MongoBus.Models.Saga;

namespace MongoBus.Internal.Saga.Activities;

internal sealed class TransitionToActivity<TInstance, TMessage>(SagaState state)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        context.Saga.CurrentState = state.Name;
        return Task.CompletedTask;
    }
}
