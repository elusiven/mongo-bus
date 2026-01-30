using MongoBus.Models;

namespace MongoBus.Abstractions;

public sealed record ConsumeMetrics(
    ConsumeContext Context,
    TimeSpan Latency);

public sealed record ConsumeFailureMetrics(
    ConsumeContext Context,
    TimeSpan Latency,
    Exception Exception);
