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
public class DelayedMessageTests(MongoDbFixture fixture)
{
    public sealed class DelayedMessage
    {
        public string Text { get; set; } = "";
    }

    public sealed class DelayedMessageHandler : IMessageHandler<DelayedMessage>
    {
        public static readonly List<DelayedMessage> ReceivedMessages = new();

        public Task HandleAsync(DelayedMessage message, ConsumeContext context, CancellationToken ct)
        {
            ReceivedMessages.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class DelayedDefinition : ConsumerDefinition<DelayedMessageHandler, DelayedMessage>
    {
        public override string TypeId => "delayed.message";
    }

    [Fact]
    public async Task PublishedDelayedMessage_ShouldNotBeProcessedBeforeTime()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "delayed_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusConsumer<DelayedMessageHandler, DelayedMessage, DelayedDefinition>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Wait for binding
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            DelayedMessageHandler.ReceivedMessages.Clear();

            // Act
            var deliverAt = DateTime.UtcNow.AddSeconds(5);
            await bus.PublishAsync("delayed.message", new DelayedMessage { Text = "I am late" }, deliverAt: deliverAt);

            // Assert: Verify it's in the inbox but pending
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var msg = await inbox.Find(x => x.TypeId == "delayed.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();
            msg.Status.Should().Be("Pending");
            msg.VisibleUtc.Should().BeCloseTo(deliverAt, TimeSpan.FromMilliseconds(100));

            // Wait 2 seconds, should NOT be processed yet
            await Task.Delay(2000);
            DelayedMessageHandler.ReceivedMessages.Should().BeEmpty();

            // Wait until after deliverAt
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && DelayedMessageHandler.ReceivedMessages.Count == 0)
            {
                await Task.Delay(100);
            }

            // Should be processed now
            DelayedMessageHandler.ReceivedMessages.Should().ContainSingle()
                .Which.Text.Should().Be("I am late");
            
            var processedMsg = await inbox.Find(x => x.Status == "Processed").FirstOrDefaultAsync();
            processedMsg.Should().NotBeNull();
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
