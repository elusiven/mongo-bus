using MongoBus.Models;

namespace MongoBus.Abstractions.Saga;

/// <summary>
/// Context provided to saga activities during behavior chain execution.
/// Provides access to the saga instance, triggering message, and bus for publishing.
/// </summary>
public sealed class SagaConsumeContext<TInstance, TMessage>
    where TInstance : class, ISagaInstance
{
    public TInstance Saga { get; }
    public TMessage Message { get; }
    public ConsumeContext Context { get; }
    public CancellationToken CancellationToken { get; }

    internal IMessageBus Bus { get; }

    internal SagaConsumeContext(
        TInstance saga,
        TMessage message,
        ConsumeContext context,
        IMessageBus bus,
        CancellationToken cancellationToken)
    {
        Saga = saga;
        Message = message;
        Context = context;
        Bus = bus;
        CancellationToken = cancellationToken;
    }
}

/// <summary>
/// Context provided to saga exception handlers during Catch activity execution.
/// Extends the regular context with access to the caught exception.
/// </summary>
public sealed class SagaExceptionContext<TInstance, TMessage, TException>
    where TInstance : class, ISagaInstance
    where TException : Exception
{
    public TInstance Saga { get; }
    public TMessage Message { get; }
    public TException Exception { get; }
    public ConsumeContext Context { get; }
    public CancellationToken CancellationToken { get; }

    internal IMessageBus Bus { get; }

    internal SagaExceptionContext(
        TInstance saga,
        TMessage message,
        TException exception,
        ConsumeContext context,
        IMessageBus bus,
        CancellationToken cancellationToken)
    {
        Saga = saga;
        Message = message;
        Exception = exception;
        Context = context;
        Bus = bus;
        CancellationToken = cancellationToken;
    }
}
