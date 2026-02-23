namespace MongoBus.DependencyInjection;

public sealed class MongoBusOptions
{
    public string ConnectionString { get; set; } = default!;
    public string DatabaseName { get; set; } = "mongo_bus";
    public string? DefaultSource { get; set; }

    public ClaimCheckOptions ClaimCheck { get; set; } = new();
    public OutboxOptions Outbox { get; set; } = new();

    /// <summary>
    /// Optional hook to route selected typeIds through the outbox when calling IMessageBus.PublishAsync.
    /// Returning true routes to outbox instead of direct inbox insertion.
    /// </summary>
    public Func<string, bool>? UseOutboxForTypeId { get; set; }

    /// <summary>
    /// How long to keep messages in the inbox after they are created.
    /// This is enforced by a MongoDB TTL index. Defaults to 7 days.
    /// </summary>
    public TimeSpan ProcessedMessageTtl { get; set; } = TimeSpan.FromDays(7);
}
