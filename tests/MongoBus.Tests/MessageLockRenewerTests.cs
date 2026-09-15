using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class MessageLockRenewerTests(MongoDbFixture fixture)
{
    private const string ThisConsumer = "this-consumer";
    private const string EndpointId = "lock-renewer-endpoint";

    [Fact]
    public async Task TryExtend_MovesTheLockForward_WhenThisConsumerStillOwnsIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeTrue();
        (await ReloadAsync(inbox, message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow.AddSeconds(25));
    }

    [Fact]
    public async Task TryExtend_LeavesTheLockAlone_WhenAnotherConsumerTookIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));
        await InboxLocks.TakeLockAsync(inbox, message.Id);

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeFalse();
        (await ReloadAsync(inbox, message)).LockOwner.Should().Be(InboxLocks.OtherOwner);
    }

    [Fact]
    public async Task TryExtend_ReportsFailure_WhenTheMessageIsNoLongerPending()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));
        await inbox.UpdateOneAsync(
            x => x.Id == message.Id,
            Builders<InboxMessage>.Update.Set(x => x.Status, InboxStatus.Processed));

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeFalse();
    }

    private static string NewDatabaseName() => "lock_renewer_" + Guid.NewGuid().ToString("N");

    private IMongoCollection<InboxMessage> InboxIn(string databaseName, string? applicationName = null) =>
        new MongoClient(new MongoUrlBuilder(fixture.ConnectionString) { ApplicationName = applicationName }.ToString())
            .GetDatabase(databaseName)
            .GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static MessageLockRenewer NewRenewer(IMongoCollection<InboxMessage> inbox) =>
        new(inbox, NullLogger.Instance);

    private static async Task<InboxMessage> InsertLockedAsync(IMongoCollection<InboxMessage> inbox, TimeSpan lockTime)
    {
        var now = DateTime.UtcNow;
        var message = new InboxMessage
        {
            Id = ObjectId.GenerateNewId(),
            EndpointId = EndpointId,
            Topic = "lock.renewer",
            TypeId = "lock.renewer",
            PayloadJson = "{}",
            CreatedUtc = now,
            VisibleUtc = now,
            LockOwner = ThisConsumer,
            LockedUntilUtc = now.Add(lockTime),
            Status = InboxStatus.Pending
        };
        await inbox.InsertOneAsync(message);
        return message;
    }

    private static Task<InboxMessage> ReloadAsync(IMongoCollection<InboxMessage> inbox, InboxMessage message) =>
        inbox.Find(x => x.Id == message.Id).SingleAsync();
}
