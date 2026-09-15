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
public class ConsumerResilienceTests(MongoDbFixture fixture)
{
    private const int InjectedFailures = 3;
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(20);

    public sealed record ResilienceMessage(string Text);

    public sealed class RecordingHandler : IMessageHandler<ResilienceMessage>
    {
        public static readonly ConcurrentBag<string> Received = [];

        public Task HandleAsync(ResilienceMessage message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message.Text);
            return Task.CompletedTask;
        }
    }

    public sealed class RecordingDefinition : ConsumerDefinition<RecordingHandler, ResilienceMessage>
    {
        public override string TypeId => "resilience.single";
    }

    public sealed class RecordingBatchHandler : IBatchMessageHandler<ResilienceMessage>
    {
        public static readonly ConcurrentBag<string> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<ResilienceMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            foreach (var message in messages)
                Received.Add(message.Text);
            return Task.CompletedTask;
        }
    }

    public sealed class RecordingBatchDefinition : BatchConsumerDefinition<RecordingBatchHandler, ResilienceMessage>
    {
        public override string TypeId => "resilience.batch";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 3,
            MaxBatchWaitTime = TimeSpan.FromMilliseconds(200),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public async Task Consumer_Should_Keep_Consuming_After_Fetching_Messages_Fails()
    {
        var applicationName = NewApplicationName();
        await using var bus = await RunningBus.StartAsync(ConnectionStringFor(applicationName),
            registerServices: services => services.AddMongoBusConsumer<RecordingHandler, ResilienceMessage, RecordingDefinition>());
        await using var failures = await FindAndModifyFailures.InjectAsync(fixture.ConnectionString, applicationName, InjectedFailures);
        var text = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>().PublishAsync("resilience.single", new ResilienceMessage(text));

        await WaitUntilAsync(() => Task.FromResult(RecordingHandler.Received.Contains(text)),
            "the consumer should deliver the message once MongoDB accepts commands again");
    }

    [Fact]
    public async Task Batch_Consumer_Should_Keep_Consuming_After_Fetching_Messages_Fails()
    {
        var applicationName = NewApplicationName();
        await using var bus = await RunningBus.StartAsync(ConnectionStringFor(applicationName),
            registerServices: services => services.AddMongoBusBatchConsumer<RecordingBatchHandler, ResilienceMessage, RecordingBatchDefinition>());
        await using var failures = await FindAndModifyFailures.InjectAsync(fixture.ConnectionString, applicationName, InjectedFailures);
        var text = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>().PublishAsync("resilience.batch", new ResilienceMessage(text));

        await WaitUntilAsync(() => Task.FromResult(RecordingBatchHandler.Received.Contains(text)),
            "the batch consumer should deliver the message once MongoDB accepts commands again");
    }

    [Fact]
    public async Task Outbox_Relay_Should_Keep_Relaying_After_Fetching_Messages_Fails()
    {
        var applicationName = NewApplicationName();
        await using var bus = await RunningBus.StartAsync(ConnectionStringFor(applicationName),
            configureOptions: opt => opt.Outbox.Enabled = true);
        await bus.Services.GetRequiredService<ITopologyManager>().BindAsync("resilience-endpoint", "resilience.outbox");
        await using var failures = await FindAndModifyFailures.InjectAsync(fixture.ConnectionString, applicationName, InjectedFailures);

        await bus.Services.GetRequiredService<ITransactionalMessageBus>()
            .PublishToOutboxAsync("resilience.outbox", new ResilienceMessage("relayed"));

        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        await WaitUntilAsync(() => inbox.Find(x => x.TypeId == "resilience.outbox").AnyAsync(),
            "the relay should move the message to the inbox once MongoDB accepts commands again");
    }

    private static string NewApplicationName() => "resilience-" + Guid.NewGuid().ToString("N");

    private string ConnectionStringFor(string applicationName) =>
        new MongoUrlBuilder(fixture.ConnectionString) { ApplicationName = applicationName }.ToString();

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string because)
    {
        var deadline = DateTime.UtcNow.Add(DeliveryTimeout);
        while (!await condition() && DateTime.UtcNow < deadline)
            await Task.Delay(100);

        (await condition()).Should().BeTrue(because);
    }

    /// <summary>
    /// Makes MongoDB reject the next findAndModify commands from one application.
    /// A plain command error is used rather than a step-down error such as ShutdownInProgress:
    /// those make the driver mark the server unknown, and the test would then measure the
    /// driver's rediscovery instead of whether the bus keeps polling.
    /// </summary>
    private sealed class FindAndModifyFailures(IMongoDatabase admin) : IAsyncDisposable
    {
        private const int BadValue = 2;

        public static async Task<FindAndModifyFailures> InjectAsync(string connectionString, string applicationName, int times)
        {
            var admin = new MongoClient(connectionString).GetDatabase("admin");
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = new BsonDocument("times", times),
                ["data"] = new BsonDocument
                {
                    ["failCommands"] = new BsonArray { "findAndModify" },
                    ["errorCode"] = BadValue,
                    ["appName"] = applicationName
                }
            });
            return new FindAndModifyFailures(admin);
        }

        public async ValueTask DisposeAsync() =>
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = "off"
            });
    }
}
