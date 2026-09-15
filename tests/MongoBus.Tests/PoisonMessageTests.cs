using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class PoisonMessageTests(MongoDbFixture fixture)
{
    private const string SingleTypeId = "poison.single";
    private const string BatchTypeId = "poison.batch";
    private const string MalformedPayload = "{ this is not json";
    private static readonly TimeSpan OutcomeTimeout = TimeSpan.FromSeconds(15);

    public sealed record PoisonTestMessage(string Text);

    public sealed class RecordingHandler : IMessageHandler<PoisonTestMessage>
    {
        public Task HandleAsync(PoisonTestMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class SingleAttemptDefinition : ConsumerDefinition<RecordingHandler, PoisonTestMessage>
    {
        public override string TypeId => SingleTypeId;
        public override int MaxAttempts => 1;
    }

    public sealed class RecordingBatchHandler : IBatchMessageHandler<PoisonTestMessage>
    {
        public static readonly ConcurrentBag<string> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<PoisonTestMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            foreach (var message in messages)
                Received.Add(message.Text);
            return Task.CompletedTask;
        }
    }

    public sealed class SingleAttemptBatchDefinition : BatchConsumerDefinition<RecordingBatchHandler, PoisonTestMessage>
    {
        public override string TypeId => BatchTypeId;
        public override int MaxAttempts => 1;
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 2,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public async Task Message_With_A_Malformed_Payload_Should_Be_Dead_Lettered()
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString,
            registerServices: s => s.AddMongoBusConsumer<RecordingHandler, PoisonTestMessage, SingleAttemptDefinition>());
        var poison = MalformedMessageFor(new SingleAttemptDefinition().EndpointName, SingleTypeId);

        await Inbox(bus).InsertOneAsync(poison);

        var stored = await WaitForStatusAsync(bus, poison.Id, "Dead");
        stored.Status.Should().Be("Dead", "a payload that cannot be parsed will never be handled, so it must not stay locked and be retried forever");
        stored.LastError.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Message_With_A_Malformed_Payload_Should_Not_Stop_The_Rest_Of_Its_Batch_Being_Handled()
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString,
            registerServices: s => s.AddMongoBusBatchConsumer<RecordingBatchHandler, PoisonTestMessage, SingleAttemptBatchDefinition>());
        var poison = MalformedMessageFor(new SingleAttemptBatchDefinition().EndpointName, BatchTypeId);
        var wellFormedText = Guid.NewGuid().ToString("N");

        await Inbox(bus).InsertOneAsync(poison);
        await bus.Services.GetRequiredService<IMessageBus>().PublishAsync(BatchTypeId, new PoisonTestMessage(wellFormedText));

        var stored = await WaitForStatusAsync(bus, poison.Id, "Dead");
        stored.Status.Should().Be("Dead", "a payload that cannot be parsed will never be handled");
        await WaitUntilAsync(() => RecordingBatchHandler.Received.Contains(wellFormedText));
        RecordingBatchHandler.Received.Should().Contain(wellFormedText, "a malformed message must not stop the well-formed messages it was batched with");
    }

    private static InboxMessage MalformedMessageFor(string endpointId, string typeId)
    {
        var dueAlready = DateTime.UtcNow.AddSeconds(-1);
        return new InboxMessage
        {
            EndpointId = endpointId,
            Topic = typeId,
            TypeId = typeId,
            PayloadJson = MalformedPayload,
            CreatedUtc = dueAlready,
            VisibleUtc = dueAlready,
            Status = "Pending",
            CloudEventId = Guid.NewGuid().ToString("N")
        };
    }

    private static IMongoCollection<InboxMessage> Inbox(RunningBus bus) =>
        bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static async Task<InboxMessage> WaitForStatusAsync(RunningBus bus, ObjectId id, string status)
    {
        InboxMessage stored = null!;
        await WaitUntilAsync(async () =>
        {
            stored = await Inbox(bus).Find(x => x.Id == id).SingleAsync();
            return stored.Status == status;
        });
        return stored;
    }

    private static Task WaitUntilAsync(Func<bool> condition) => WaitUntilAsync(() => Task.FromResult(condition()));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.Add(OutcomeTimeout);
        while (!await condition() && DateTime.UtcNow < deadline)
            await Task.Delay(100);
    }
}
