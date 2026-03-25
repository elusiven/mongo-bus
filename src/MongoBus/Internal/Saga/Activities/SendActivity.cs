using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

/// <summary>
/// Sends a message to a specific endpoint via targeted publish.
/// Uses the existing PublishAsync with the target endpoint's typeId binding.
/// </summary>
internal sealed class SendActivity<TInstance, TMessage>(
    string endpointName,
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, object> factory)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = factory(context);
        await context.Bus.PublishAsync(
            typeId,
            data,
            subject: endpointName,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);
    }
}

internal sealed class SendAsyncActivity<TInstance, TMessage>(
    string endpointName,
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<object>> factory)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = await factory(context);
        await context.Bus.PublishAsync(
            typeId,
            data,
            subject: endpointName,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);
    }
}
