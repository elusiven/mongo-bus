using MongoDB.Bson;

namespace MongoBus.Models.Saga;

/// <summary>
/// Records a single state transition in a saga's lifecycle for audit/debugging.
/// </summary>
public sealed class SagaHistoryEntry
{
    public ObjectId Id { get; set; }
    public string CorrelationId { get; set; } = default!;
    public string SagaType { get; set; } = default!;
    public string PreviousState { get; set; } = default!;
    public string NewState { get; set; } = default!;
    public string EventTypeId { get; set; } = default!;
    public string? MessageId { get; set; }
    public DateTime TimestampUtc { get; set; }
    public int VersionAfter { get; set; }
}
