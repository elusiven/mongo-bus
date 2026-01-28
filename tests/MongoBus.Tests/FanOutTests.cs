using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class FanOutTests(MongoDbFixture fixture)
{
    public sealed class SharedMessage
    {
        public string Content { get; set; } = "";
    }

    public sealed class FirstHandler : IMessageHandler<SharedMessage>
    {
        public static int Count;
        public Task HandleAsync(SharedMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }

    public sealed class SecondHandler : IMessageHandler<SharedMessage>
    {
        public static int Count;
        public Task HandleAsync(SharedMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Count);
            return Task.CompletedTask;
        }
    }

    public sealed class FirstDefinition : ConsumerDefinition<FirstHandler, SharedMessage>
    {
        public override string TypeId => "shared.message";
        public override string EndpointName => "endpoint-1";
    }

    public sealed class SecondDefinition : ConsumerDefinition<SecondHandler, SharedMessage>
    {
        public override string TypeId => "shared.message";
        public override string EndpointName => "endpoint-2";
    }

    [Fact]
    public async Task PublishOneMessage_ShouldDeliverToBothEndpoints()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "fanout_test_" + Guid.NewGuid().ToString("N");
        });
        
        // Add both consumers
        services.AddMongoBusConsumer<FirstHandler, SharedMessage, FirstDefinition>();
        services.AddMongoBusConsumer<SecondHandler, SharedMessage, SecondDefinition>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            FirstHandler.Count = 0;
            SecondHandler.Count = 0;

            // Wait for bindings
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && await bindings.CountDocumentsAsync(x => x.Topic == "shared.message") < 2)
            {
                await Task.Delay(100);
            }

            // Act
            await bus.PublishAsync("shared.message", new SharedMessage { Content = "Multi" }, "test-source");

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && (FirstHandler.Count == 0 || SecondHandler.Count == 0))
            {
                await Task.Delay(100);
            }

            FirstHandler.Count.Should().Be(1);
            SecondHandler.Count.Should().Be(1);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
