namespace MongoBus.Abstractions;

public sealed record PublishMetrics(
    string TypeId,
    int EndpointCount,
    TimeSpan Latency);

public sealed record PublishFailureMetrics(
    string TypeId,
    int EndpointCount,
    TimeSpan Latency,
    Exception Exception);
