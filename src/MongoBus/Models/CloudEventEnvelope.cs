namespace MongoBus.Models;

public sealed class CloudEventEnvelope<TData>
{
    public string SpecVersion { get; set; } = "1.0";
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Type { get; set; } = default!;
    public string Source { get; set; } = default!;
    public DateTime Time { get; set; } = DateTime.UtcNow;
    public string? Subject { get; set; }
    public string? DataContentType { get; set; }
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? TraceParent { get; set; }
    public string? TraceState { get; set; }
    public TData Data { get; set; } = default!;
}
