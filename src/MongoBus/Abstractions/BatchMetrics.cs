namespace MongoBus.Abstractions;

public sealed record BatchMetrics(
    string EndpointId,
    string TypeId,
    int BatchSize,
    TimeSpan Latency,
    string? GroupKey,
    BatchFlushMode FlushMode);

public sealed record BatchFailureMetrics(
    string EndpointId,
    string TypeId,
    int BatchSize,
    TimeSpan Latency,
    BatchFailureMode FailureMode,
    Exception Exception);
