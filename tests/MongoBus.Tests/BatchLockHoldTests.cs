using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class BatchLockHoldTests(MongoDbFixture fixture)
{
    private const string TrickleTypeId = "batch.lockhold.message";
    private static readonly TimeSpan BatchLockTime = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DispatchTimeout = TimeSpan.FromSeconds(25);

    public sealed record TrickleMessage(int Index);

    public sealed class TrickleHandler : IBatchMessageHandler<TrickleMessage>
    {
        public static readonly ConcurrentQueue<int> BatchSizes = new();

        public Task HandleBatchAsync(IReadOnlyList<TrickleMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            BatchSizes.Enqueue(messages.Count);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Waits for a batch of five, but only two messages are ever published. The batch therefore never reaches
    /// <c>MinBatchSize</c>, and holds both messages' locks while it waits for a third that never arrives.
    /// </summary>
    public sealed class TrickleDefinition : BatchConsumerDefinition<TrickleHandler, TrickleMessage>
    {
        public override string TypeId => TrickleTypeId;
        public override TimeSpan LockTime => BatchLockTime;
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 5,
            MaxBatchSize = 50,
            MaxBatchWaitTime = TimeSpan.Zero,
            MaxBatchIdleTime = TimeSpan.FromSeconds(2),
            FlushMode = BatchFlushMode.SinceLastMessage
        };
    }

    [Fact]
    public async Task ShouldDispatchBeforeTheLocksExpireWhenFewerThanMinBatchSizeArrive()
    {
        while (TrickleHandler.BatchSizes.TryDequeue(out _)) { }

        await using var bus = await RunningBus.StartAsync(
            fixture.ConnectionString,
            registerServices: services =>
                services.AddMongoBusBatchConsumer<TrickleHandler, TrickleMessage, TrickleDefinition>());

        var messageBus = bus.Services.GetRequiredService<IMessageBus>();
        await messageBus.PublishAsync(TrickleTypeId, new TrickleMessage(1), "test-source");
        await messageBus.PublishAsync(TrickleTypeId, new TrickleMessage(2), "test-source");

        var dispatchedBatchSize = await WaitForFirstBatchAsync();

        dispatchedBatchSize.Should().Be(2,
            "a batch that never reaches MinBatchSize still holds its messages' locks, so it has to be dispatched "
            + "before those locks expire and a competing consumer takes the messages");
    }

    [Fact]
    public void ShouldRejectAnIdleTimeThatOutlastsTheLock()
    {
        var definitions = new IConsumerDefinition[] { new IdleOutlastsLockDefinition() };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxBatchIdleTime*LockTime*");
    }

    [Fact]
    public void ShouldRejectAWaitTimeThatOutlastsTheLock()
    {
        var definitions = new IConsumerDefinition[] { new WaitOutlastsLockDefinition() };

        var act = () => MongoBusConfigValidator.ValidateDefinitions(definitions);

        act.Should().Throw<InvalidOperationException>().WithMessage("*MaxBatchWaitTime*LockTime*");
    }

    public sealed class UnusedHandler : IBatchMessageHandler<TrickleMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<TrickleMessage> messages, BatchConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class IdleOutlastsLockDefinition : BatchConsumerDefinition<UnusedHandler, TrickleMessage>
    {
        public override string TypeId => "batch.idle.outlasts.lock";
        public override string EndpointName => "batch-idle-outlasts-lock";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.Zero,
            MaxBatchIdleTime = TimeSpan.FromSeconds(60),
            FlushMode = BatchFlushMode.SinceLastMessage
        };
    }

    public sealed class WaitOutlastsLockDefinition : BatchConsumerDefinition<UnusedHandler, TrickleMessage>
    {
        public override string TypeId => "batch.wait.outlasts.lock";
        public override string EndpointName => "batch-wait-outlasts-lock";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.FromSeconds(60),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    /// <returns>The size of the first dispatched batch, or 0 when none was dispatched in time.</returns>
    private static async Task<int> WaitForFirstBatchAsync()
    {
        var deadline = DateTime.UtcNow.Add(DispatchTimeout);
        while (DateTime.UtcNow < deadline)
        {
            if (TrickleHandler.BatchSizes.TryPeek(out var size))
                return size;

            await Task.Delay(100);
        }

        return 0;
    }
}
