namespace MongoBus.DependencyInjection;

public sealed class MongoBusOptions
{
    public string ConnectionString { get; set; } = default!;
    public string DatabaseName { get; set; } = "mongo_bus";
    public string? DefaultSource { get; set; }

    /// <summary>
    /// How long to keep messages in the inbox after they are created.
    /// This is enforced by a MongoDB TTL index. Defaults to 7 days.
    /// </summary>
    public TimeSpan ProcessedMessageTtl { get; set; } = TimeSpan.FromDays(7);
}
