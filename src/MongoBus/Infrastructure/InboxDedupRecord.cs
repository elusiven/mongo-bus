namespace MongoBus.Infrastructure;

/// <summary>
/// One consumer endpoint's claim on one CloudEvent. Its <see cref="Id"/> is the claim itself: inserting it is what
/// reserves the event, so two consumers racing on the same copy cannot both win. Claims expire on the same schedule
/// as processed inbox messages.
/// </summary>
public sealed class InboxDedupRecord
{
    public string Id { get; set; } = default!;
    public DateTime CreatedUtc { get; set; }
}
