using MongoDB.Bson;

namespace MongoBus.Models;

public sealed record ConsumeContext(
    string EndpointId,
    string TypeId,
    ObjectId MessageId,
    int Attempt,
    string? Subject,
    string Source,
    string CloudEventId,
    string? CorrelationId = null,
    string? CausationId = null);
