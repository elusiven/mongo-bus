namespace MongoBus.Internal;

/// <summary>
/// How long a polling loop waits after an unexpected failure, such as MongoDB being unreachable
/// during a primary step-down. The delay doubles on each consecutive failure, up to a cap, so an
/// outage is not hammered, and starts over after the next success.
/// </summary>
internal sealed class FailureBackoff
{
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaximumDelay = TimeSpan.FromSeconds(10);

    private TimeSpan _nextDelay = InitialDelay;

    public TimeSpan NextDelay()
    {
        var delay = _nextDelay;
        _nextDelay = TimeSpan.FromTicks(Math.Min(_nextDelay.Ticks * 2, MaximumDelay.Ticks));
        return delay;
    }

    public void Reset() => _nextDelay = InitialDelay;
}
