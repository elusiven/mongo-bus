namespace MongoBus.Models;

public sealed record BatchConsumeContext(
    string EndpointId,
    string TypeId,
    IReadOnlyList<ConsumeContext> Messages,
    DateTime BatchStartedUtc,
    DateTime BatchCompletedUtc);
