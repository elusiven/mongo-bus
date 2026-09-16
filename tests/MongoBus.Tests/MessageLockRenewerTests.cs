using System.Collections.Concurrent;
using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.Logging;
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
    private static readonly TimeSpan LeaseLockTime = TimeSpan.FromSeconds(9);
    private static readonly TimeSpan GiveUpBudget = LeaseLockTime - LeaseLockTime / 6;
    private static readonly TimeSpan SchedulingTolerance = TimeSpan.FromSeconds(1);

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

    [Fact]
    public async Task Lease_KeepsTheLockAlive_ForLongerThanLockTime()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(15));

        lease.Should().NotBeNull();
        lease!.LockLost.IsCancellationRequested.Should().BeFalse();
        (await ReloadAsync(inbox, message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow);
    }

    [Fact]
    public async Task Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock()
    {
        var inbox = InboxIn(NewDatabaseName());
        var lockTime = TimeSpan.FromSeconds(9);
        var message = await InsertLockedAsync(inbox, lockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, lockTime, CancellationToken.None);
        await InboxLocks.TakeLockAsync(inbox, message.Id);
        var signalled = await WaitForCancellationAsync(lease!.LockLost, TimeSpan.FromSeconds(5));

        signalled.Should().BeTrue();
        (await ReloadAsync(inbox, message)).LockOwner.Should().Be(InboxLocks.OtherOwner);
    }

    [Fact]
    public async Task TryAcquireLease_ReturnsNoLease_WhenTheLockIsAlreadyTaken()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(30));
        await InboxLocks.TakeLockAsync(inbox, message.Id);

        var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        lease.Should().BeNull();
    }

    [Fact]
    public async Task MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses()
    {
        var databaseName = NewDatabaseName();
        var inbox = InboxIn(databaseName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);
        var pump = new MongoMessagePump(new MongoClient(fixture.ConnectionString).GetDatabase(databaseName));

        var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(12));
        await lease!.DisposeAsync();
        var storedExpiry = (await ReloadAsync(inbox, message)).LockedUntilUtc!.Value;

        var beforeLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);
        var untilLapse = storedExpiry - DateTime.UtcNow + TimeSpan.FromMilliseconds(250);
        await Task.Delay(untilLapse > TimeSpan.Zero ? untilLapse : TimeSpan.Zero);
        var afterLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);

        beforeLapse.Should().BeNull();
        afterLapse.Should().NotBeNull();
        afterLapse!.Id.Should().Be(message.Id);
    }

    [Fact]
    public async Task Lease_KeepsTheLock_WhenOneRenewalFails()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(
            fixture.ConnectionString, applicationName, new BsonDocument("times", 1));
        await Task.Delay(TimeSpan.FromSeconds(12));

        lease!.LockLost.IsCancellationRequested.Should().BeFalse();
        (await ReloadAsync(InboxIn(databaseName), message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow);
    }

    [Fact]
    public async Task Lease_SignalsLockLostBeforeTheLockExpires_WhenRenewalsKeepFailing()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        var sinceClaim = Stopwatch.StartNew();
        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(20));

        sinceClaim.Elapsed.Should().BeLessThan(GiveUpBudget + SchedulingTolerance);
    }

    [Fact]
    public async Task Lease_SignalsLockLostBeforeTheLockExpires_WhenARenewalHangs()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        var sinceClaim = Stopwatch.StartNew();
        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(
            fixture.ConnectionString, applicationName, "alwaysOn", blockMilliseconds: 10_000);
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(20));

        sinceClaim.Elapsed.Should().BeLessThan(GiveUpBudget + SchedulingTolerance);
    }

    [Fact]
    public async Task Lease_WarnsThatItGaveUp_WhenRenewalsKeepFailing()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);
        var log = new RecordingLogger();

        await using var lease = await new MessageLockRenewer(inbox, log).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(20));
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        log.Warnings.Should().ContainSingle(warning => warning.Contains("neared expiry") && warning.Contains(message.Id.ToString()));
    }

    [Fact]
    public async Task Lease_WarnsOnlyThatTheLockWasTaken_WhenAnotherConsumerTakesIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var lockTime = TimeSpan.FromSeconds(9);
        var message = await InsertLockedAsync(inbox, lockTime);
        var log = new RecordingLogger();

        await using var lease = await new MessageLockRenewer(inbox, log).TryAcquireLeaseAsync(message, lockTime, CancellationToken.None);
        await InboxLocks.TakeLockAsync(inbox, message.Id);
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        log.Warnings.Should().ContainSingle().Which.Should().Contain("no longer holds the lock");
    }

    /// <summary>
    /// Guards the guard in <c>ReportGivingUp</c>. The assertion holds either way — cancellation keeps invoking the
    /// remaining callbacks after one throws — so the regression signal is the run itself: unguarded, the logger's
    /// exception is unhandled on the watchdog's timer thread and fails the run with a non-zero exit code.
    /// </summary>
    [Fact]
    public async Task Lease_StillGivesUp_WhenTheLoggerThrows()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await new MessageLockRenewer(inbox, new LoggerThatFailsOnGiveUp())
            .TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");

        var signalled = await WaitForCancellationAsync(lease!.LockLost, TimeSpan.FromSeconds(20));

        signalled.Should().BeTrue();
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

    private static string NewApplicationName() => "lock-renewer-" + Guid.NewGuid().ToString("N");

    private static async Task<bool> WaitForCancellationAsync(CancellationToken token, TimeSpan timeout)
    {
        try
        {
            await Task.Delay(timeout, token);
            return false;
        }
        catch (OperationCanceledException)
        {
            return true;
        }
    }

    /// <summary>When <paramref name="token"/> was cancelled; throws <see cref="TimeoutException"/> if it was not.</summary>
    private static async Task<DateTime> CancellationTimeAsync(CancellationToken token, TimeSpan timeout)
    {
        var cancelledAt = new TaskCompletionSource<DateTime>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = token.Register(() => cancelledAt.TrySetResult(DateTime.UtcNow));
        return await cancelledAt.Task.WaitAsync(timeout);
    }

    /// <summary>
    /// Makes MongoDB reject update commands from one application until disposed, optionally holding each rejected
    /// command's connection first. A plain command error is used, as in <c>ConsumerResilienceTests</c>, so the driver
    /// does not mark the server unknown.
    /// </summary>
    private sealed class UpdateFailures(IMongoDatabase admin) : IAsyncDisposable
    {
        private const int BadValue = 2;

        public static async Task<UpdateFailures> InjectAsync(
            string connectionString, string applicationName, BsonValue mode, int? blockMilliseconds = null)
        {
            var data = new BsonDocument
            {
                ["failCommands"] = new BsonArray { "update" },
                ["errorCode"] = BadValue,
                ["appName"] = applicationName
            };
            if (blockMilliseconds is { } block)
            {
                data["blockConnection"] = true;
                data["blockTimeMS"] = block;
            }

            var admin = new MongoClient(connectionString).GetDatabase("admin");
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = mode,
                ["data"] = data
            });
            return new UpdateFailures(admin);
        }

        public async ValueTask DisposeAsync() =>
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = "off"
            });
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

        public IEnumerable<string> Warnings =>
            _entries.Where(entry => entry.Level == LogLevel.Warning).Select(entry => entry.Message);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _entries.Enqueue((logLevel, formatter(state, exception)));
    }

    /// <summary>Fails only on the give-up warning, so the renewal loop's own logging is unaffected.</summary>
    private sealed class LoggerThatFailsOnGiveUp : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).Contains("neared expiry"))
                throw new InvalidOperationException("logging provider failed");
        }
    }
}
