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
