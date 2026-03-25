namespace MongoBus.Models.Saga;

/// <summary>
/// Thrown when a saga instance update fails due to optimistic concurrency conflict.
/// This exception triggers the retry mechanism in the dispatcher.
/// </summary>
public sealed class SagaConcurrencyException : Exception
{
    public string CorrelationId { get; }
    public int ExpectedVersion { get; }

    public SagaConcurrencyException(string correlationId, int expectedVersion)
        : base($"Saga instance '{correlationId}' was modified concurrently. Expected version {expectedVersion}.")
    {
        CorrelationId = correlationId;
        ExpectedVersion = expectedVersion;
    }

    public SagaConcurrencyException(string correlationId, int expectedVersion, Exception innerException)
        : base($"Saga instance '{correlationId}' was modified concurrently. Expected version {expectedVersion}.", innerException)
    {
        CorrelationId = correlationId;
        ExpectedVersion = expectedVersion;
    }
}
