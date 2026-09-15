namespace MongoBus.DependencyInjection;

public sealed class OutboxOptions
{
    public bool Enabled { get; set; }
    /// <summary>
    /// How long to keep an outbox message after it has been relayed to the inbox.
    /// This is enforced by a MongoDB TTL index on <c>PublishedUtc</c>, so pending, delayed and
    /// dead-lettered outbox messages are never expired. Defaults to 7 days.
    /// </summary>
    public TimeSpan ProcessedMessageTtl { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan LockTime { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxAttempts { get; set; } = 10;
}
