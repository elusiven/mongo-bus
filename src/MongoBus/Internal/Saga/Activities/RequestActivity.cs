using MongoBus.Abstractions.Saga;
using MongoBus.Models.Saga;

namespace MongoBus.Internal.Saga.Activities;

/// <summary>
/// Publishes a request message and schedules a timeout for the response.
/// Stores the request ID on the saga instance for correlation.
/// </summary>
internal sealed class RequestActivity<TInstance, TMessage, TRequest, TResponse>(
    SagaRequest<TInstance, TRequest, TResponse> request,
    Func<SagaConsumeContext<TInstance, TMessage>, TRequest> factory,
    Action<TInstance, string?> requestIdSetter)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = factory(context);
        var requestId = Guid.NewGuid().ToString("N");

        requestIdSetter(context.Saga, requestId);

        // Publish the request
        await context.Bus.PublishAsync(
            request.RequestTypeId,
            data,
            id: requestId,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);

        // Schedule the timeout if configured
        if (request.Timeout > TimeSpan.Zero)
        {
            var timeoutData = new SagaTimeoutMessage
            {
                CorrelationId = context.Saga.CorrelationId,
                RequestId = requestId
            };

            await context.Bus.PublishAsync(
                request.RequestTypeId + ".timeout",
                timeoutData,
                deliverAt: DateTime.UtcNow.Add(request.Timeout),
                correlationId: context.Saga.CorrelationId,
                causationId: context.Context.CloudEventId,
                ct: context.CancellationToken);
        }

        // Transition to the Pending state
        context.Saga.CurrentState = request.Pending.Name;
    }
}

internal sealed class RequestAsyncActivity<TInstance, TMessage, TRequest, TResponse>(
    SagaRequest<TInstance, TRequest, TResponse> request,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<TRequest>> factory,
    Action<TInstance, string?> requestIdSetter)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = await factory(context);
        var requestId = Guid.NewGuid().ToString("N");

        requestIdSetter(context.Saga, requestId);

        await context.Bus.PublishAsync(
            request.RequestTypeId,
            data,
            id: requestId,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            ct: context.CancellationToken);

        if (request.Timeout > TimeSpan.Zero)
        {
            var timeoutData = new SagaTimeoutMessage
            {
                CorrelationId = context.Saga.CorrelationId,
                RequestId = requestId
            };

            await context.Bus.PublishAsync(
                request.RequestTypeId + ".timeout",
                timeoutData,
                deliverAt: DateTime.UtcNow.Add(request.Timeout),
                correlationId: context.Saga.CorrelationId,
                causationId: context.Context.CloudEventId,
                ct: context.CancellationToken);
        }

        context.Saga.CurrentState = request.Pending.Name;
    }
}
