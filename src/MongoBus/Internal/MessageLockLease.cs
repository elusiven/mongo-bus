using Microsoft.Extensions.Logging;
using MongoBus.Infrastructure;

namespace MongoBus.Internal;

/// <summary>
/// Keeps one delivery's lock on a message until disposed, renewing every third of the lock time, and signals
/// <see cref="LockLost"/> when the lock was taken or can no longer be counted on.
/// </summary>
internal sealed class MessageLockLease : IAsyncDisposable
{
    private readonly CancellationTokenSource _lockLost = new();
    private readonly CancellationTokenSource _stopRenewing;
    private readonly Task _renewing;

    private MessageLockLease(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log, CancellationToken ct)
    {
        _stopRenewing = CancellationTokenSource.CreateLinkedTokenSource(ct);
        GiveUpBeforeExpiry(claimedAt, lockTime);
        _renewing = RenewUntilStoppedAsync(renewer, message, lockTime, log, _stopRenewing.Token);
    }

    /// <summary>Cancelled once this delivery can no longer count on holding the lock; work on the message should stop.</summary>
    public CancellationToken LockLost => _lockLost.Token;

    /// <param name="claimedAt">When the claim that confirmed the lock was sent; the first deadline counts from here.</param>
    public static MessageLockLease StartRenewing(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log, CancellationToken ct) =>
        new(renewer, message, lockTime, claimedAt, log, ct);

    /// <summary>
    /// Schedules <see cref="LockLost"/> a sixth of the lock time before the confirmed expiry. It fires whether or not a
    /// renewal is still in flight, so a slow or hanging write cannot keep the handler running into the moment another
    /// consumer may take the message; one failed renewal in between is still tolerated.
    /// </summary>
    private void GiveUpBeforeExpiry(DateTime extendedAt, TimeSpan lockTime)
    {
        var deadline = extendedAt.Add(lockTime - lockTime / 6);
        var remaining = deadline - DateTime.UtcNow;
        _lockLost.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
    }

    private async Task RenewUntilStoppedAsync(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, ILogger log, CancellationToken stop)
    {
        var interval = lockTime / 3;
        while (!stop.IsCancellationRequested && !_lockLost.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stop);

                var attemptedAt = DateTime.UtcNow;
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(stop, _lockLost.Token);
                if (await renewer.TryExtendAsync(message, lockTime, attempt.Token))
                {
                    GiveUpBeforeExpiry(attemptedAt, lockTime);
                    continue;
                }

                log.LogWarning(
                    "Delivery no longer holds the lock on message {MessageId} on endpoint {EndpointId}; cancelling its dispatch.",
                    message.Id, message.EndpointId);
                await _lockLost.CancelAsync();
                return;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested || _lockLost.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                log.LogWarning(
                    ex,
                    "Could not renew the lock on message {MessageId}; its dispatch is cancelled if the lock nears expiry first.",
                    message.Id);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stopRenewing.CancelAsync();
        await _renewing;
        _stopRenewing.Dispose();
        _lockLost.Dispose();
    }
}
