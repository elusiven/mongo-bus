using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class LockRenewalTests(MongoDbFixture fixture)
{
    public sealed class HeldMessage
    {
        public string Name { get; set; } = "";
    }

    public sealed class HeldHandler : IMessageHandler<HeldMessage>
    {
        public static ConcurrentQueue<string> Started = new();
        public static TaskCompletionSource Release = NewSignal();

        public static void Reset()
        {
            Started = new ConcurrentQueue<string>();
            Release = NewSignal();
        }

        public async Task HandleAsync(HeldMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.Enqueue(message.Name);
            await Release.Task.WaitAsync(ct);
        }
    }

    public sealed class HeldDefinition : ConsumerDefinition<HeldHandler, HeldMessage>
    {
        public override string TypeId => "renewal.held";
        public override int ConcurrencyLimit => 2;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task EachLockTakenByARenewingEndpoint_HasItsOwnOwner()
    {
        HeldHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<HeldHandler, HeldMessage, HeldDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.held", new HeldMessage { Name = "a" }, "test-source");
        await PublisherOf(bus).PublishAsync("renewal.held", new HeldMessage { Name = "b" }, "test-source");
        await WaitUntilAsync(() => Task.FromResult(HeldHandler.Started.Count == 2), TimeSpan.FromSeconds(10));

        var owners = await InboxOf(bus).Find(x => x.TypeId == "renewal.held").Project(x => x.LockOwner).ToListAsync();
        HeldHandler.Release.TrySetResult();

        owners.Should().HaveCount(2).And.NotContainNulls().And.OnlyHaveUniqueItems();
    }

    public sealed class SlowPlainMessage { }

    public sealed class SlowPlainHandler : IMessageHandler<SlowPlainMessage>
    {
        public static int Starts;

        public async Task HandleAsync(SlowPlainMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Starts);
            await Task.Delay(TimeSpan.FromSeconds(2.5), ct);
        }
    }

    public sealed class SlowPlainDefinition : ConsumerDefinition<SlowPlainHandler, SlowPlainMessage>
    {
        public override string TypeId => "renewal.slow-plain";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(1);
    }

    /// <summary>
    /// Guards the opt-in: a consumer that does not renew keeps one owner per pump, so the first of its overlapping
    /// copies to finish still records the outcome. Per-lock owners here would redeliver the message forever.
    /// </summary>
    [Fact]
    public async Task NonRenewingHandlerOutlivingLockTime_StillGetsItsMessageProcessed()
    {
        Interlocked.Exchange(ref SlowPlainHandler.Starts, 0);
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<SlowPlainHandler, SlowPlainMessage, SlowPlainDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.slow-plain", new SlowPlainMessage(), "test-source");
        await WaitUntilAsync(
            async () => await InboxOf(bus).CountDocumentsAsync(x => x.TypeId == "renewal.slow-plain" && x.Status == InboxStatus.Processed) == 1,
            TimeSpan.FromSeconds(15));

        SlowPlainHandler.Starts.Should().BeLessThanOrEqualTo(5);
    }

    public sealed class LongMessage { }

    public sealed class LongHandler : IMessageHandler<LongMessage>
    {
        public static int Starts;
        public static int Ends;

        public async Task HandleAsync(LongMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Starts);
            await Task.Delay(TimeSpan.FromSeconds(15), ct);
            Interlocked.Increment(ref Ends);
        }
    }

    public sealed class LongDefinition : ConsumerDefinition<LongHandler, LongMessage>
    {
        public override string TypeId => "renewal.long";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(9);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task HandlerOutlivingLockTime_RunsOnce_AcrossCompetingConsumers()
    {
        Interlocked.Exchange(ref LongHandler.Starts, 0);
        Interlocked.Exchange(ref LongHandler.Ends, 0);
        var databaseName = NewDatabaseName();
        Action<IServiceCollection> registerConsumer =
            services => services.AddMongoBusConsumer<LongHandler, LongMessage, LongDefinition>();
        await using var first = await StartBusAsync(databaseName, registerConsumer);
        await using var second = await StartBusAsync(databaseName, registerConsumer);

        await PublisherOf(first).PublishAsync("renewal.long", new LongMessage(), "test-source");
        await WaitUntilAsync(() => Task.FromResult(LongHandler.Ends >= 1), TimeSpan.FromSeconds(40));
        await Task.Delay(TimeSpan.FromSeconds(12));

        LongHandler.Starts.Should().Be(1);
        (await InboxOf(first).Find(x => x.TypeId == "renewal.long").SingleAsync()).Status.Should().Be(InboxStatus.Processed);
    }

    public sealed class StolenMessage { }

    public sealed class StolenHandler : IMessageHandler<StolenMessage>
    {
        public static TaskCompletionSource Started = NewSignal();
        public static TaskCompletionSource Cancelled = NewSignal();

        public static void Reset()
        {
            Started = NewSignal();
            Cancelled = NewSignal();
        }

        public async Task HandleAsync(StolenMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                Cancelled.TrySetResult();
                throw;
            }
        }
    }

    public sealed class StolenDefinition : ConsumerDefinition<StolenHandler, StolenMessage>
    {
        public override string TypeId => "renewal.stolen";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(9);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task RenewingHandler_IsCancelled_WhenAnotherConsumerTakesItsLock()
    {
        StolenHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<StolenHandler, StolenMessage, StolenDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.stolen", new StolenMessage(), "test-source");
        await StolenHandler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var message = await InboxOf(bus).Find(x => x.TypeId == "renewal.stolen").SingleAsync();
        await InboxLocks.TakeLockAsync(InboxOf(bus), message.Id);

        await StolenHandler.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var stored = await InboxOf(bus).Find(x => x.Id == message.Id).SingleAsync();
        stored.LockOwner.Should().Be(InboxLocks.OtherOwner);
        stored.Status.Should().Be(InboxStatus.Pending);
        stored.Attempt.Should().Be(message.Attempt);
    }

    public sealed class BacklogMessage
    {
        public string Name { get; set; } = "";
    }

    public sealed class BacklogHandler : IMessageHandler<BacklogMessage>
    {
        public static ConcurrentQueue<string> Handled = new();
        public static TaskCompletionSource ReleaseBlockers = NewSignal();

        public static void Reset()
        {
            Handled = new ConcurrentQueue<string>();
            ReleaseBlockers = NewSignal();
        }

        public async Task HandleAsync(BacklogMessage message, ConsumeContext context, CancellationToken ct)
        {
            Handled.Enqueue(message.Name);
            if (message.Name.StartsWith("blocker", StringComparison.Ordinal))
                await ReleaseBlockers.Task.WaitAsync(ct);
        }
    }

    public sealed class BacklogDefinition : ConsumerDefinition<BacklogHandler, BacklogMessage>
    {
        public override string TypeId => "renewal.backlog";
        public override int ConcurrencyLimit => 2;
        public override int PrefetchCount => 2;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(9);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task WaitingMessageRelockedByItsOwnEndpoint_IsHandledOnce()
    {
        BacklogHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<BacklogHandler, BacklogMessage, BacklogDefinition>());
        var inbox = InboxOf(bus);

        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "blocker-1" }, "test-source");
        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "blocker-2" }, "test-source");
        await WaitUntilAsync(() => Task.FromResult(BacklogHandler.Handled.Count == 2), TimeSpan.FromSeconds(10));
        var blockerIds = await inbox.Find(x => x.TypeId == "renewal.backlog").Project(x => x.Id).ToListAsync();

        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "waiting" }, "test-source");
        InboxMessage? firstLock = null;
        await WaitUntilAsync(
            async () => (firstLock = await inbox.Find(x => !blockerIds.Contains(x.Id) && x.LockOwner != null).FirstOrDefaultAsync()) != null,
            TimeSpan.FromSeconds(10));
        var waitingId = firstLock!.Id;
        var firstOwner = firstLock.LockOwner;
        await WaitUntilAsync(
            async () => await inbox.CountDocumentsAsync(x => x.Id == waitingId && x.LockOwner != firstOwner) == 1,
            TimeSpan.FromSeconds(30));

        BacklogHandler.ReleaseBlockers.TrySetResult();
        await WaitUntilAsync(() => Task.FromResult(BacklogHandler.Handled.Contains("waiting")), TimeSpan.FromSeconds(45));
        await Task.Delay(TimeSpan.FromSeconds(12));

        BacklogHandler.Handled.Count(name => name == "waiting").Should().Be(1);
    }

    public sealed class FinishingMessage { }

    public sealed class FinishingHandler : IMessageHandler<FinishingMessage>
    {
        public static TaskCompletionSource Started = NewSignal();
        public static TaskCompletionSource Release = NewSignal();

        public static void Reset()
        {
            Started = NewSignal();
            Release = NewSignal();
        }

        public async Task HandleAsync(FinishingMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.TrySetResult();
            await Release.Task;
        }
    }

    public sealed class FinishingDefinition : ConsumerDefinition<FinishingHandler, FinishingMessage>
    {
        public override string TypeId => "renewal.finishing";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(6);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns()
    {
        FinishingHandler.Reset();
        var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<FinishingHandler, FinishingMessage, FinishingDefinition>());
        var inbox = InboxOf(bus);
        Task? stopping = null;
        InboxMessage locked;
        InboxMessage whileStopping;
        DateTime readAt;
        try
        {
            await PublisherOf(bus).PublishAsync("renewal.finishing", new FinishingMessage(), "test-source");
            await FinishingHandler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            locked = await inbox.Find(x => x.TypeId == "renewal.finishing").SingleAsync();

            stopping = bus.DisposeAsync().AsTask();
            await Task.Delay(TimeSpan.FromSeconds(8));
            readAt = DateTime.UtcNow;
            whileStopping = await inbox.Find(x => x.Id == locked.Id).SingleAsync();
        }
        finally
        {
            FinishingHandler.Release.TrySetResult();
            await (stopping ?? bus.DisposeAsync().AsTask()).WaitAsync(TimeSpan.FromSeconds(15));
        }

        whileStopping.LockOwner.Should().Be(locked.LockOwner);
        whileStopping.LockedUntilUtc.Should().BeAfter(readAt);
        (await inbox.Find(x => x.Id == locked.Id).SingleAsync()).Status.Should().Be(InboxStatus.Processed);
    }

    private Task<RunningBus> StartBusAsync(string databaseName, Action<IServiceCollection> registerConsumer) =>
        RunningBus.StartAsync(fixture.ConnectionString, options => options.DatabaseName = databaseName, registerConsumer);

    private static IMessageBus PublisherOf(RunningBus bus) => bus.Services.GetRequiredService<IMessageBus>();

    private static IMongoCollection<InboxMessage> InboxOf(RunningBus bus) =>
        bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static string NewDatabaseName() => "lock_renewal_" + Guid.NewGuid().ToString("N");

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met in time.");
            await Task.Delay(50);
        }
    }
}
