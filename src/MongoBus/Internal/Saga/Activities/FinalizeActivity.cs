using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

internal sealed class FinalizeActivity<TInstance, TMessage>
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    internal const string FinalStateName = "Final";

    public Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        context.Saga.CurrentState = FinalStateName;
        return Task.CompletedTask;
    }
}
