using MongoDB.Bson;

namespace MongoBus.Infrastructure;

public sealed class OutboxMessage
{
    public ObjectId Id { get; set; }
    public string Topic { get; set; } = default!;
    public string TypeId { get; set; } = default!;
    public string PayloadJson { get; set; } = default!;
    public DateTime CreatedUtc { get; set; }
    public DateTime VisibleUtc { get; set; }
    public DateTime? LockedUntilUtc { get; set; }
    public string? LockOwner { get; set; }
    public int Attempt { get; set; }
    public string Status { get; set; } = "Pending";
    public DateTime? PublishedUtc { get; set; }
    public string? LastError { get; set; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? CloudEventId { get; set; }
}
