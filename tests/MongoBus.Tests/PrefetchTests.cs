using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class PrefetchTests(MongoDbFixture fixture)
{
    private const string BlockingTypeId = "prefetch.blocking.message";
    private const int ConcurrencyLimit = 1;
    private const int PublishedMessages = 10;

    /// <summary>
    /// A message may be locked for each busy worker, for each prefetched message waiting in the channel, and for the
    /// one the fetch loop has locked but not yet handed over. Prefetching defaults to the concurrency limit.
    /// </summary>
    private const int MaxLockedMessages = ConcurrencyLimit * 2 + 1;

    private static readonly TimeSpan HandlerStartTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan LockObservationWindow = TimeSpan.FromSeconds(3);

    public sealed class BlockingMessage { }

    public sealed class BlockingHandler : IMessageHandler<BlockingMessage>
    {
        public static TaskCompletionSource Released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public static int StartCount;

        public async Task HandleAsync(BlockingMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref StartCount);
            await Released.Task.WaitAsync(ct);
        }
    }

    public sealed class BlockingDefinition : ConsumerDefinition<BlockingHandler, BlockingMessage>
    {
        public override string TypeId => BlockingTypeId;
        public override int ConcurrencyLimit => PrefetchTests.ConcurrencyLimit;
    }

    [Fact]
    public async Task ShouldNotLockMoreMessagesThanItsWorkersCanHold()
    {
        Interlocked.Exchange(ref BlockingHandler.StartCount, 0);
        BlockingHandler.Released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var bus = await RunningBus.StartAsync(
            fixture.ConnectionString,
            registerServices: services => services.AddMongoBusConsumer<BlockingHandler, BlockingMessage, BlockingDefinition>());

        try
        {
            var messageBus = bus.Services.GetRequiredService<IMessageBus>();
            for (var i = 0; i < PublishedMessages; i++)
                await messageBus.PublishAsync(BlockingTypeId, new BlockingMessage(), "test-source");

            await WaitUntilHandlerStartedAsync();
            var peakLocked = await ObservePeakLockedMessagesAsync(bus.Database);

            peakLocked.Should().BeLessThanOrEqualTo(MaxLockedMessages,
                "a prefetched message holds its lock while it waits, so locking far more than the workers can handle "
                + "lets those locks lapse and hands the messages to a competing consumer");
        }
        finally
        {
            BlockingHandler.Released.TrySetResult();
        }
    }

    private static async Task WaitUntilHandlerStartedAsync()
    {
        var deadline = DateTime.UtcNow.Add(HandlerStartTimeout);
        while (Volatile.Read(ref BlockingHandler.StartCount) == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"No '{BlockingTypeId}' message was handled within {HandlerStartTimeout}.");

            await Task.Delay(50);
        }
    }

    private static async Task<long> ObservePeakLockedMessagesAsync(IMongoDatabase db)
    {
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadline = DateTime.UtcNow.Add(LockObservationWindow);
        long peak = 0;

        while (DateTime.UtcNow < deadline)
        {
            var locked = await inbox.CountDocumentsAsync(x => x.TypeId == BlockingTypeId && x.LockOwner != null);
            peak = Math.Max(peak, locked);
            await Task.Delay(100);
        }

        return peak;
    }
}
