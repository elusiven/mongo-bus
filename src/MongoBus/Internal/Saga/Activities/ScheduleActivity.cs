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

        // Store the schedule token on the instance for potential unschedule
        SetTokenProperty(context.Saga, scheduleId);

        await context.Bus.PublishAsync(
            schedule.TypeId,
            data,
            deliverAt: deliverAt,
            correlationId: context.Saga.CorrelationId,
            causationId: context.Context.CloudEventId,
            id: scheduleId,
            ct: context.CancellationToken);
    }

    private static void SetTokenProperty(TInstance instance, string token)
    {
        // Token property is set via the schedule's token expression at registration time.
        // The BehaviorBuilder passes a setter action when creating this activity.
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

/// <summary>
/// Unschedules a previously scheduled timeout by clearing the token.
/// Since MongoBus uses delayed delivery without cancellation support,
/// the actual timeout message will still arrive but will be ignored
/// via state guards in the behavior configuration.
/// </summary>
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
