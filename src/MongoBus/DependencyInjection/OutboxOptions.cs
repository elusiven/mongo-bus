namespace MongoBus.DependencyInjection;

public sealed class OutboxOptions
{
    public bool Enabled { get; set; }
    public TimeSpan ProcessedMessageTtl { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan LockTime { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxAttempts { get; set; } = 10;
}
