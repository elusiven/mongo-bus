using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class BusIntegrationTests(MongoDbFixture fixture)
{
    public sealed class TestMessage
    {
        public string Text { get; set; } = "";
    }

    public sealed class TestMessageHandler : IMessageHandler<TestMessage>
    {
        public static readonly List<TestMessage> ReceivedMessages = new();

        public Task HandleAsync(TestMessage message, ConsumeContext context, CancellationToken ct)
        {
            ReceivedMessages.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class TestMessageConsumerDefinition : ConsumerDefinition<TestMessageHandler, TestMessage>
    {
        public override string TypeId => "test.message";
    }

    public sealed class TestPublishObserver : IPublishObserver
    {
        public static int PublishCount;

        public void OnPublish(PublishMetrics metrics)
        {
            Interlocked.Increment(ref PublishCount);
        }

        public void OnPublishFailed(PublishFailureMetrics metrics)
        {
        }
    }

    public sealed class TestConsumeObserver : IConsumeObserver
    {
        public static int ConsumeCount;

        public void OnMessageProcessed(ConsumeMetrics metrics)
        {
            Interlocked.Increment(ref ConsumeCount);
        }

        public void OnMessageFailed(ConsumeFailureMetrics metrics)
        {
        }
    }

    [Fact]
    public async Task FullBusFlow_ShouldWork()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "test_bus_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusConsumer<TestMessageHandler, TestMessage, TestMessageConsumerDefinition>();
        
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        // Start hosted services (indexes + runtime)
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Verify indexes were created
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var bindings = db.GetCollection<Binding>("bus_bindings");

            var inboxIndexes = await (await inbox.Indexes.ListAsync()).ToListAsync();
            inboxIndexes.Should().HaveCountGreaterThan(1);

            // Wait for binding to be registered by the runtime
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            var bindingCount = await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty);
            bindingCount.Should().BeGreaterThan(0, "Bindings should be registered by the runtime");

            // Act
            TestMessageHandler.ReceivedMessages.Clear();
            await bus.PublishAsync("test.message", new TestMessage { Text = "Hello World" }, "test-source");

            // Assert
            // Wait for processing
            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout && TestMessageHandler.ReceivedMessages.Count == 0)
            {
                await Task.Delay(100);
            }

            TestMessageHandler.ReceivedMessages.Should().ContainSingle()
                .Which.Text.Should().Be("Hello World");

            var processedMsg = await inbox.Find(x => x.Status == "Processed").FirstOrDefaultAsync();
            processedMsg.Should().NotBeNull();
            processedMsg.Attempt.Should().Be(0);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Observers_ShouldBeInvoked_ForPublishAndConsume()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "test_observers_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusConsumer<TestMessageHandler, TestMessage, TestMessageConsumerDefinition>();
        services.AddMongoBusPublishObserver<TestPublishObserver>();
        services.AddMongoBusConsumeObserver<TestConsumeObserver>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            TestPublishObserver.PublishCount = 0;
            TestConsumeObserver.ConsumeCount = 0;

            await bus.PublishAsync("test.message", new TestMessage { Text = "Observer Test" }, "test-source");

            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout && TestConsumeObserver.ConsumeCount == 0)
            {
                await Task.Delay(100);
            }

            TestPublishObserver.PublishCount.Should().Be(1);
            TestConsumeObserver.ConsumeCount.Should().Be(1);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
