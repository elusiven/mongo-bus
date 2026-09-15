using MongoBus.Abstractions.Saga;
using MongoBus.Models.Saga;

namespace MongoBus.Internal.Saga.Activities;

/// <summary>
/// Schedules a timeout message for future delivery using delayed delivery (deliverAt).
/// </summary>
internal sealed class ScheduleActivity<TInstance, TMessage, TTimeout>(
    SagaSchedule<TInstance, TTimeout> schedule,
    Func<SagaConsumeContext<TInstance, TMessage>, TTimeout> factory,
    TimeSpan? delayOverride)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = factory(context);
        var delay = delayOverride ?? schedule.Delay;
        var deliverAt = DateTime.UtcNow.Add(delay);
        var scheduleId = Guid.NewGuid().ToString("N");

        await context.Bus.PublishAsync(
            schedule.TypeId,
            data,
            deliverAt: deliverAt,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            id: scheduleId,
            ct: context.CancellationToken);
    }
}

/// <summary>
/// Extended version that includes a token setter for tracking scheduled message IDs.
/// </summary>
internal sealed class ScheduleWithTokenActivity<TInstance, TMessage, TTimeout>(
    SagaSchedule<TInstance, TTimeout> schedule,
    Func<SagaConsumeContext<TInstance, TMessage>, TTimeout> factory,
    Action<TInstance, string?> tokenSetter,
    TimeSpan? delayOverride)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = factory(context);
        var delay = delayOverride ?? schedule.Delay;
        var deliverAt = DateTime.UtcNow.Add(delay);
        var scheduleId = Guid.NewGuid().ToString("N");

        tokenSetter(context.Saga, scheduleId);

        await context.Bus.PublishAsync(
            schedule.TypeId,
            data,
            deliverAt: deliverAt,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            id: scheduleId,
            ct: context.CancellationToken);
    }
}

internal sealed class ScheduleAsyncActivity<TInstance, TMessage, TTimeout>(
    SagaSchedule<TInstance, TTimeout> schedule,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<TTimeout>> factory,
    TimeSpan? delayOverride)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = await factory(context);
        var delay = delayOverride ?? schedule.Delay;
        var deliverAt = DateTime.UtcNow.Add(delay);
        var scheduleId = Guid.NewGuid().ToString("N");

        await context.Bus.PublishAsync(
            schedule.TypeId,
            data,
            deliverAt: deliverAt,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            id: scheduleId,
            ct: context.CancellationToken);
    }
}

internal sealed class ScheduleWithTokenAsyncActivity<TInstance, TMessage, TTimeout>(
    SagaSchedule<TInstance, TTimeout> schedule,
    Func<SagaConsumeContext<TInstance, TMessage>, Task<TTimeout>> factory,
    Action<TInstance, string?> tokenSetter,
    TimeSpan? delayOverride)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        var data = await factory(context);
        var delay = delayOverride ?? schedule.Delay;
        var deliverAt = DateTime.UtcNow.Add(delay);
        var scheduleId = Guid.NewGuid().ToString("N");

        tokenSetter(context.Saga, scheduleId);

        await context.Bus.PublishAsync(
            schedule.TypeId,
            data,
            deliverAt: deliverAt,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            id: scheduleId,
            ct: context.CancellationToken);
    }
}

/// <summary>
/// Clears a schedule token. Delayed delivery cannot be cancelled, so the scheduled message still arrives;
/// it is discarded if the saga has completed or its current state does not handle it.
/// </summary>
internal sealed class UnscheduleActivity<TInstance, TMessage, TTimeout>(
    Action<TInstance, string?> tokenSetter)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        tokenSetter(context.Saga, null);
        return Task.CompletedTask;
    }
}
