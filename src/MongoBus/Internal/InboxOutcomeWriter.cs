using Microsoft.Extensions.Logging;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

/// <summary>
/// Records what happened to messages a worker handled, but only for the messages that worker still holds
/// the lock on. If a lock expired and another worker took a message over, that worker owns its outcome.
/// Writes ignore the stopping token, so messages handled while the bus stops are still recorded instead of
/// being delivered again; the timeout keeps an unreachable server from holding up shutdown.
/// </summary>
internal sealed class InboxOutcomeWriter(IMongoCollection<InboxMessage> inbox, ILogger log)
{
    private static readonly TimeSpan WriteTimeout = TimeSpan.FromSeconds(10);

    public async Task RecordAsync(IReadOnlyList<InboxMessage> messages, UpdateDefinition<InboxMessage> outcome)
    {
        var lockedByThisWorker = Builders<InboxMessage>.Filter.Or(messages.Select(LockedByThisWorker));

        using var timeout = new CancellationTokenSource(WriteTimeout);
        var result = await inbox.UpdateManyAsync(lockedByThisWorker, outcome, cancellationToken: timeout.Token);

        if (result.MatchedCount < messages.Count)
        {
            log.LogWarning(
                "{Unrecorded} of {Count} messages on endpoint {EndpointId} are no longer locked by this worker; their outcome was not recorded.",
                messages.Count - result.MatchedCount,
                messages.Count,
                messages[0].EndpointId);
        }
    }

    private static FilterDefinition<InboxMessage> LockedByThisWorker(InboxMessage message) =>
        Builders<InboxMessage>.Filter.Eq(x => x.Id, message.Id)
        & Builders<InboxMessage>.Filter.Eq(x => x.LockOwner, message.LockOwner);
}
