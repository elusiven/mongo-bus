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
public class BatchBackpressureLockTests(MongoDbFixture fixture)
{
    private const string BlockingTypeId = "batch.backpressure.lockhold";
    private const int ConcurrencyLimit = 5;
    private const int MaxInFlightBatches = 1;
    private const int MaxBatchSize = 1;
    private const int PublishedMessages = 10;

    /// <summary>
    /// Only the batches allowed in flight should hold locks. Every other worker should be waiting for a slot before
    /// it locks anything.
    /// </summary>
    private const int MaxLockedMessages = MaxInFlightBatches * MaxBatchSize;

    private static readonly TimeSpan HandlerStartTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan LockObservationWindow = TimeSpan.FromSeconds(3);

    public sealed record BlockingBatchMessage(int Index);

    public sealed class BlockingBatchHandler : IBatchMessageHandler<BlockingBatchMessage>
    {
        public static TaskCompletionSource Released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public static int StartCount;

        public async Task HandleBatchAsync(
            IReadOnlyList<BlockingBatchMessage> messages,
            BatchConsumeContext context,
            CancellationToken ct)
        {
            Interlocked.Increment(ref StartCount);
            await Released.Task.WaitAsync(ct);
        }
    }

    public sealed class BlockingBatchDefinition : BatchConsumerDefinition<BlockingBatchHandler, BlockingBatchMessage>
    {
        public override string TypeId => BlockingTypeId;
        public override int ConcurrencyLimit => BatchBackpressureLockTests.ConcurrencyLimit;
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = MaxBatchSize,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage,
            MaxInFlightBatches = BatchBackpressureLockTests.MaxInFlightBatches
        };
    }

    [Fact]
    public async Task ShouldNotLockMessagesWhileWaitingForAnInFlightSlot()
    {
        Interlocked.Exchange(ref BlockingBatchHandler.StartCount, 0);
        BlockingBatchHandler.Released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var bus = await RunningBus.StartAsync(
            fixture.ConnectionString,
            registerServices: services =>
                services.AddMongoBusBatchConsumer<BlockingBatchHandler, BlockingBatchMessage, BlockingBatchDefinition>());

        try
        {
            var messageBus = bus.Services.GetRequiredService<IMessageBus>();
            for (var i = 0; i < PublishedMessages; i++)
                await messageBus.PublishAsync(BlockingTypeId, new BlockingBatchMessage(i), "test-source");

            await WaitUntilHandlerStartedAsync();
            var peakLocked = await ObservePeakLockedMessagesAsync(bus.Database);

            peakLocked.Should().BeLessThanOrEqualTo(MaxLockedMessages,
                "a worker that cannot dispatch yet should wait for its in-flight slot before locking anything; "
                + "locking first leaves those messages locked for an unbounded time, until their locks lapse and a "
                + "competing consumer takes them");
        }
        finally
        {
            BlockingBatchHandler.Released.TrySetResult();
        }
    }

    private static async Task WaitUntilHandlerStartedAsync()
    {
        var deadline = DateTime.UtcNow.Add(HandlerStartTimeout);
        while (Volatile.Read(ref BlockingBatchHandler.StartCount) == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"No '{BlockingTypeId}' batch was handled within {HandlerStartTimeout}.");

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
