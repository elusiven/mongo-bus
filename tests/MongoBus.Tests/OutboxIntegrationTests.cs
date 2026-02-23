using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class OutboxIntegrationTests(MongoDbFixture fixture)
{
    public sealed class OutboxMessageModel
    {
        public string Text { get; set; } = "";
    }

    public sealed class FirstOutboxHandler : IMessageHandler<OutboxMessageModel>
    {
        public static int Count;

        public Task HandleAsync(OutboxMessageModel message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }

    public sealed class SecondOutboxHandler : IMessageHandler<OutboxMessageModel>
    {
        public static int Count;

        public Task HandleAsync(OutboxMessageModel message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }

    public sealed class OutboxDefinition : ConsumerDefinition<FirstOutboxHandler, OutboxMessageModel>
    {
        public override string TypeId => "outbox.test.message";
        public override string EndpointName => "outbox-single-endpoint";
    }

    public sealed class OutboxFanoutDefinition1 : ConsumerDefinition<FirstOutboxHandler, OutboxMessageModel>
    {
        public override string TypeId => "outbox.test.fanout";
        public override string EndpointName => "outbox-fanout-1";
    }

    public sealed class OutboxFanoutDefinition2 : ConsumerDefinition<SecondOutboxHandler, OutboxMessageModel>
    {
        public override string TypeId => "outbox.test.fanout";
        public override string EndpointName => "outbox-fanout-2";
    }

    [Fact]
    public async Task PublishToOutbox_ShouldStorePendingMessage()
    {
        var services = BuildServices();
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var outbox = sp.GetRequiredService<IMongoDatabase>().GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);

        await transactionalBus.PublishToOutboxAsync("outbox.test.message", new OutboxMessageModel { Text = "hello" });

        var msg = await outbox.Find(x => x.Topic == "outbox.test.message").FirstOrDefaultAsync();
        msg.Should().NotBeNull();
        msg!.Status.Should().Be("Pending");
        msg.PayloadJson.Should().Contain("hello");
    }

    [Fact]
    public async Task PublishAsync_WhenHookEnabled_ShouldRouteToOutbox()
    {
        var services = BuildServices(opt => opt.UseOutboxForTypeId = _ => true);
        var sp = services.BuildServiceProvider();

        var bus = sp.GetRequiredService<IMessageBus>();
        var outbox = sp.GetRequiredService<IMongoDatabase>().GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);

        await bus.PublishAsync("outbox.test.message", new OutboxMessageModel { Text = "routed" });

        var msg = await outbox.Find(x => x.Topic == "outbox.test.message").FirstOrDefaultAsync();
        msg.Should().NotBeNull();
        msg!.PayloadJson.Should().Contain("routed");
    }

    [Fact]
    public async Task Relay_ShouldPublishToInboxAndProcessMessage()
    {
        var services = BuildServices();
        services.AddMongoBusConsumer<FirstOutboxHandler, OutboxMessageModel, OutboxDefinition>();
        var sp = services.BuildServiceProvider();

        var db = sp.GetRequiredService<IMongoDatabase>();
        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var hostedServices = await StartHostedServicesAsync(sp);

        try
        {
            FirstOutboxHandler.Count = 0;
            await WaitForBindingsAsync(db, "outbox.test.message", expectedCount: 1);

            await transactionalBus.PublishToOutboxAsync("outbox.test.message", new OutboxMessageModel { Text = "processed" });

            await WaitUntilAsync(() => FirstOutboxHandler.Count == 1);

            var published = await outbox.Find(x => x.Topic == "outbox.test.message" && x.Status == "Published").FirstOrDefaultAsync();
            published.Should().NotBeNull();

            var processed = await inbox.Find(x => x.TypeId == "outbox.test.message" && x.Status == "Processed").FirstOrDefaultAsync();
            processed.Should().NotBeNull();
        }
        finally
        {
            await StopHostedServicesAsync(hostedServices);
        }
    }

    [Fact]
    public async Task Relay_ShouldRespectDelayedDelivery()
    {
        var services = BuildServices();
        services.AddMongoBusConsumer<FirstOutboxHandler, OutboxMessageModel, OutboxDefinition>();
        var sp = services.BuildServiceProvider();

        var db = sp.GetRequiredService<IMongoDatabase>();
        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var hostedServices = await StartHostedServicesAsync(sp);

        try
        {
            FirstOutboxHandler.Count = 0;
            await WaitForBindingsAsync(db, "outbox.test.message", expectedCount: 1);

            await transactionalBus.PublishToOutboxAsync(
                "outbox.test.message",
                new OutboxMessageModel { Text = "delayed" },
                deliverAt: DateTime.UtcNow.AddSeconds(4));

            await Task.Delay(1500);
            FirstOutboxHandler.Count.Should().Be(0);

            await WaitUntilAsync(() => FirstOutboxHandler.Count == 1, timeoutSeconds: 15);
        }
        finally
        {
            await StopHostedServicesAsync(hostedServices);
        }
    }

    [Fact]
    public async Task Relay_ShouldFanOutToAllBoundEndpoints()
    {
        var services = BuildServices();
        services.AddMongoBusConsumer<FirstOutboxHandler, OutboxMessageModel, OutboxFanoutDefinition1>();
        services.AddMongoBusConsumer<SecondOutboxHandler, OutboxMessageModel, OutboxFanoutDefinition2>();
        var sp = services.BuildServiceProvider();

        var db = sp.GetRequiredService<IMongoDatabase>();
        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        var hostedServices = await StartHostedServicesAsync(sp);

        try
        {
            FirstOutboxHandler.Count = 0;
            SecondOutboxHandler.Count = 0;
            await WaitForBindingsAsync(db, "outbox.test.fanout", expectedCount: 2);

            await transactionalBus.PublishToOutboxAsync("outbox.test.fanout", new OutboxMessageModel { Text = "fanout" });

            await WaitUntilAsync(() => FirstOutboxHandler.Count == 1 && SecondOutboxHandler.Count == 1);

            var published = await outbox.Find(x => x.Topic == "outbox.test.fanout" && x.Status == "Published").FirstOrDefaultAsync();
            published.Should().NotBeNull();
        }
        finally
        {
            await StopHostedServicesAsync(hostedServices);
        }
    }

    [Fact]
    public async Task Relay_ShouldNotDuplicateInboxEntries_ForSameCloudEventId()
    {
        var services = BuildServices();
        services.AddMongoBusConsumer<FirstOutboxHandler, OutboxMessageModel, OutboxDefinition>();
        var sp = services.BuildServiceProvider();

        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var serializer = sp.GetRequiredService<ICloudEventSerializer>();
        var hostedServices = await StartHostedServicesAsync(sp);

        try
        {
            FirstOutboxHandler.Count = 0;
            await WaitForBindingsAsync(db, "outbox.test.message", expectedCount: 1);

            var cloudEventId = Guid.NewGuid().ToString("N");
            var payload = serializer.Serialize(new CloudEventEnvelope<OutboxMessageModel>
            {
                Id = cloudEventId,
                Type = "outbox.test.message",
                Source = "tests",
                Data = new OutboxMessageModel { Text = "dup-check" }
            });

            var now = DateTime.UtcNow;
            var docs = new[]
            {
                new OutboxMessage
                {
                    Topic = "outbox.test.message",
                    TypeId = "outbox.test.message",
                    PayloadJson = payload,
                    CreatedUtc = now,
                    VisibleUtc = now,
                    Status = "Pending",
                    CloudEventId = cloudEventId
                },
                new OutboxMessage
                {
                    Topic = "outbox.test.message",
                    TypeId = "outbox.test.message",
                    PayloadJson = payload,
                    CreatedUtc = now,
                    VisibleUtc = now,
                    Status = "Pending",
                    CloudEventId = cloudEventId
                }
            };

            await outbox.InsertManyAsync(docs);
            await WaitUntilAsync(() => FirstOutboxHandler.Count >= 1);
            await Task.Delay(1000);

            var inboxDocs = await inbox.Find(x => x.EndpointId == "outbox-single-endpoint" && x.CloudEventId == cloudEventId).ToListAsync();
            inboxDocs.Should().HaveCount(1);
        }
        finally
        {
            await StopHostedServicesAsync(hostedServices);
        }
    }

    [Fact]
    public async Task PublishWithTransactionAsync_ShouldCommitOutboxMessage_WhenTransactionsSupported()
    {
        var services = BuildServices();
        var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IMongoClient>();
        if (!await SupportsTransactionsAsync(client))
            return;

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        var custom = db.GetCollection<BsonDocument>("tx_data");

        await transactionalBus.PublishWithTransactionAsync(
            "outbox.test.message",
            new OutboxMessageModel { Text = "tx-commit" },
            async (session, ct) =>
            {
                await custom.InsertOneAsync(session, new BsonDocument("k", "v"), cancellationToken: ct);
            });

        (await custom.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(1);
        (await outbox.CountDocumentsAsync(x => x.Topic == "outbox.test.message")).Should().Be(1);
    }

    [Fact]
    public async Task PublishWithTransactionAsync_ShouldRollback_WhenCallbackFailsAndTransactionsSupported()
    {
        var services = BuildServices();
        var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IMongoClient>();
        if (!await SupportsTransactionsAsync(client))
            return;

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
        var custom = db.GetCollection<BsonDocument>("tx_data_fail");

        Func<Task> act = async () =>
            await transactionalBus.PublishWithTransactionAsync(
                "outbox.test.message",
                new OutboxMessageModel { Text = "tx-rollback" },
                async (session, ct) =>
                {
                    await custom.InsertOneAsync(session, new BsonDocument("k", "v"), cancellationToken: ct);
                    throw new InvalidOperationException("boom");
                });

        await act.Should().ThrowAsync<InvalidOperationException>();

        (await custom.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(0);
        (await outbox.CountDocumentsAsync(x => x.Topic == "outbox.test.message")).Should().Be(0);
    }

    private ServiceCollection BuildServices(Action<MongoBusOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "outbox_test_" + Guid.NewGuid().ToString("N");
            opt.Outbox.Enabled = true;
            opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(50);
            opt.Outbox.LockTime = TimeSpan.FromSeconds(10);
            opt.Outbox.MaxAttempts = 3;
            configure?.Invoke(opt);
        });
        return services;
    }

    private static async Task<List<IHostedService>> StartHostedServicesAsync(ServiceProvider sp)
    {
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        return hostedServices;
    }

    private static async Task StopHostedServicesAsync(IEnumerable<IHostedService> hostedServices)
    {
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitForBindingsAsync(IMongoDatabase db, string topic, int expectedCount)
    {
        var bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
        await WaitUntilAsync(async () => await bindings.CountDocumentsAsync(x => x.Topic == topic) >= expectedCount, timeoutSeconds: 10);
    }

    private static Task WaitUntilAsync(Func<bool> predicate, int timeoutSeconds = 10)
    {
        return WaitUntilAsync(() => Task.FromResult(predicate()), timeoutSeconds);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, int timeoutSeconds = 10)
    {
        var timeoutAt = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (DateTime.UtcNow < timeoutAt)
        {
            if (await predicate())
                return;

            await Task.Delay(100);
        }

        throw new TimeoutException("Condition was not met in time.");
    }

    private static async Task<bool> SupportsTransactionsAsync(IMongoClient client)
    {
        var admin = client.GetDatabase("admin");
        var hello = await admin.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1));
        return hello.Contains("setName");
    }
}
