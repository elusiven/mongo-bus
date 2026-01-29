using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MongoMessagePump(IMongoDatabase db) : IMessagePump
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    public async Task<InboxMessage?> TryLockOneAsync(string endpointId, TimeSpan lockTime, string pumpId, CancellationToken ct)
    {
        var now = DateTime.UtcNow;

        return await _inbox.FindOneAndUpdateAsync(
            BuildLockFilter(endpointId, now),
            BuildLockUpdate(lockTime, pumpId, now),
            new FindOneAndUpdateOptions<InboxMessage>
            {
                ReturnDocument = ReturnDocument.After,
                Sort = Builders<InboxMessage>.Sort.Ascending(x => x.VisibleUtc)
            },
            ct);
    }

    private static FilterDefinition<InboxMessage> BuildLockFilter(string endpointId, DateTime now) =>
        Builders<InboxMessage>.Filter.And(
            Builders<InboxMessage>.Filter.Eq(x => x.EndpointId, endpointId),
            Builders<InboxMessage>.Filter.Eq(x => x.Status, InboxStatus.Pending),
            Builders<InboxMessage>.Filter.Lt(x => x.VisibleUtc, now),
            Builders<InboxMessage>.Filter.Or(
                Builders<InboxMessage>.Filter.Eq(x => x.LockedUntilUtc, null),
                Builders<InboxMessage>.Filter.Lt(x => x.LockedUntilUtc, now)
            )
        );

    private static UpdateDefinition<InboxMessage> BuildLockUpdate(TimeSpan lockTime, string pumpId, DateTime now) =>
        Builders<InboxMessage>.Update
            .Set(x => x.LockedUntilUtc, now.Add(lockTime))
            .Set(x => x.LockOwner, pumpId);
}
