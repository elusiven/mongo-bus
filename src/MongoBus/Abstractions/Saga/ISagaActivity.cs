namespace MongoBus.Abstractions.Saga;

/// <summary>
/// Represents a single activity in a saga behavior chain.
/// Activities are executed sequentially during event handling.
/// </summary>
internal interface ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context);
}
