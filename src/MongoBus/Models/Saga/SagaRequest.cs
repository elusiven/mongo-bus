namespace MongoBus.Models.Saga;

/// <summary>
/// Represents a request/response declaration on a saga state machine.
/// Includes automatically generated sub-events for Completed, Faulted, and TimeoutExpired.
/// </summary>
public sealed class SagaRequest<TInstance, TRequest, TResponse>
    where TInstance : class, MongoBus.Abstractions.Saga.ISagaInstance
{
    public string Name { get; }
    public string RequestTypeId { get; }
    public string ResponseTypeId { get; }
    public TimeSpan Timeout { get; internal set; }

    /// <summary>
    /// Auto-generated state representing the pending request.
    /// </summary>
    public SagaState Pending { get; internal set; } = default!;

    /// <summary>
    /// Event raised when the response is received successfully.
    /// </summary>
    public SagaEvent<TResponse> Completed { get; internal set; } = default!;

    /// <summary>
    /// Event raised when the request faults.
    /// </summary>
    public SagaEvent<SagaFaultMessage> Faulted { get; internal set; } = default!;

    /// <summary>
    /// Event raised when the request times out.
    /// </summary>
    public SagaEvent<SagaTimeoutMessage> TimeoutExpired { get; internal set; } = default!;

    internal SagaRequest(string name, string requestTypeId, string responseTypeId, TimeSpan timeout)
    {
        Name = name;
        RequestTypeId = requestTypeId;
        ResponseTypeId = responseTypeId;
        Timeout = timeout;
    }
}

/// <summary>
/// Message published when a saga request faults.
/// </summary>
public sealed class SagaFaultMessage
{
    public string CorrelationId { get; set; } = default!;
    public string RequestId { get; set; } = default!;
    public string ExceptionType { get; set; } = default!;
    public string ExceptionMessage { get; set; } = default!;
}

/// <summary>
/// Message published when a saga request times out.
/// </summary>
public sealed class SagaTimeoutMessage
{
    public string CorrelationId { get; set; } = default!;
    public string RequestId { get; set; } = default!;
}
