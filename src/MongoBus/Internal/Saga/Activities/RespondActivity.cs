using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

/// <summary>
/// Publishes a response message correlated to the original request.
/// </summary>
internal sealed class RespondActivity<TInstance, TMessage, TResponse>(
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, TResponse> factory)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = factory(context);
        await context.Bus.PublishAsync(
            typeId,
            data,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);
    }
}

internal sealed class RespondAsyncActivity<TInstance, TMessage, TResponse>(
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<TResponse>> factory)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = await factory(context);
        await context.Bus.PublishAsync(
            typeId,
            data,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);
    }
}
