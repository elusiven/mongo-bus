using Microsoft.Extensions.Logging;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MessageLockRenewer(IMongoCollection<InboxMessage> inbox, ILogger log)
{
    /// <summary>
    /// Pushes the lock's expiry forward, but only while this delivery still owns the pending message.
    /// Returns false when the message was re-locked or is no longer pending.
    /// </summary>
    public async Task<bool> TryExtendAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)
    {
        var result = await inbox.UpdateOneAsync(
            x => x.Id == message.Id && x.LockOwner == message.LockOwner && x.Status == InboxStatus.Pending,
            Builders<InboxMessage>.Update.Set(x => x.LockedUntilUtc, DateTime.UtcNow.Add(lockTime)),
            cancellationToken: ct);

        return result.MatchedCount == 1;
    }

    /// <summary>
    /// Re-claims the message and starts renewing its lock, or returns null when the message was re-locked while it
    /// waited to be dispatched.
    /// </summary>
    public async Task<MessageLockLease?> TryAcquireLeaseAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)
    {
        var claimedAt = DateTime.UtcNow;
        return await TryExtendAsync(message, lockTime, ct)
            ? MessageLockLease.StartRenewing(this, message, lockTime, claimedAt, log, ct)
            : null;
    }
}
