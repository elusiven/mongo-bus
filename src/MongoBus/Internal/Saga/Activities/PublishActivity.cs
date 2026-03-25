using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

internal sealed class PublishActivity<TInstance, TMessage, TPublish>(
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, TPublish> factory)
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

internal sealed class PublishAsyncActivity<TInstance, TMessage, TPublish>(
    string typeId,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<TPublish>> factory)
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
