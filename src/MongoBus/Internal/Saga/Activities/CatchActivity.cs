using MongoBus.Abstractions.Saga;

namespace MongoBus.Internal.Saga.Activities;

/// <summary>
/// Marker exception used by the Rethrow activity to signal that the caught exception
/// should be re-thrown after the catch branch completes.
/// </summary>
internal sealed class SagaRethrowException : Exception
{
    public Exception Original { get; }

    public SagaRethrowException(Exception original)
        : base("Saga rethrow requested", original)
    {
        Original = original;
    }
}

/// <summary>
/// Wraps preceding activities in a try-catch for a specific exception type.
/// If the exception is caught, the catch branch executes.
/// </summary>
internal sealed class CatchActivity<TInstance, TMessage, TException>(
    IReadOnlyList<ISagaActivity<TInstance, TMessage>> guardedActivities,
    IReadOnlyList<ISagaExceptionActivity<TInstance, TMessage, TException>> catchBranch,
    bool rethrow)
    : ISagaActivity<TInstance, TMessage>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public async Task ExecuteAsync(SagaConsumeContext<TInstance, TMessage> context)
    {
        try
        {
            foreach (var activity in guardedActivities)
                await activity.ExecuteAsync(context);
        }
        catch (TException ex)
        {
            var exceptionContext = new SagaExceptionContext<TInstance, TMessage, TException>(
                context.Saga,
                context.Message,
                ex,
                context.Context,
                context.Bus,
                context.CancellationToken);

            foreach (var activity in catchBranch)
                await activity.ExecuteAsync(exceptionContext);

            if (rethrow)
                throw;
        }
    }
}

/// <summary>
/// Activity interface for exception handler branches.
/// </summary>
internal interface ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context);
}

/// <summary>
/// Then activity for exception handler branches.
/// </summary>
internal sealed class ExceptionThenActivity<TInstance, TMessage, TException>(
    Action<SagaExceptionContext<TInstance, TMessage, TException>> action)
    : ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context)
    {
        action(context);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Async Then activity for exception handler branches.
/// </summary>
internal sealed class ExceptionThenAsyncActivity<TInstance, TMessage, TException>(
    Func<SagaExceptionContext<TInstance, TMessage, TException>, Task> action)
    : ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context)
    {
        return action(context);
    }
}

/// <summary>
/// TransitionTo activity for exception handler branches.
/// </summary>
internal sealed class ExceptionTransitionToActivity<TInstance, TMessage, TException>(
    Models.Saga.SagaState state)
    : ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context)
    {
        context.Saga.CurrentState = state.Name;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Finalize activity for exception handler branches.
/// </summary>
internal sealed class ExceptionFinalizeActivity<TInstance, TMessage, TException>
    : ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context)
    {
        context.Saga.CurrentState = FinalizeActivity<TInstance, TMessage>.FinalStateName;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Publish activity for exception handler branches.
/// </summary>
internal sealed class ExceptionPublishActivity<TInstance, TMessage, TException, TPublish>(
    string typeId,
    Func<SagaExceptionContext<TInstance, TMessage, TException>, TPublish> factory)
    : ISagaExceptionActivity<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public async Task ExecuteAsync(SagaExceptionContext<TInstance, TMessage, TException> context)
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
