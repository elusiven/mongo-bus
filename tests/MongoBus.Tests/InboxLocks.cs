using MongoBus.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Tests;

internal static class InboxLocks
{
    public const string OtherOwner = "another-consumer";

    /// <summary>Seizes a message's lock the way a competing consumer would after the lock expired.</summary>
    public static Task TakeLockAsync(IMongoCollection<InboxMessage> inbox, ObjectId messageId) =>
        inbox.UpdateOneAsync(
            x => x.Id == messageId,
            Builders<InboxMessage>.Update
                .Set(x => x.LockOwner, OtherOwner)
                .Set(x => x.LockedUntilUtc, DateTime.UtcNow.AddMinutes(5)));
}
