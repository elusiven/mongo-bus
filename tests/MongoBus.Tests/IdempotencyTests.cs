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
public class IdempotencyTests(MongoDbFixture fixture)
{
    public sealed class IdempotentMessage
    {
        public string Value { get; set; } = "";
    }

    public sealed class IdempotentHandler : IMessageHandler<IdempotentMessage>
    {
        public static int HandleCount;
        public Task HandleAsync(IdempotentMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref HandleCount);
            return Task.CompletedTask;
        }
    }

    public sealed class IdempotentDefinition : ConsumerDefinition<IdempotentHandler, IdempotentMessage>
    {
        public override string TypeId => "idempotent.message";
        public override bool IdempotencyEnabled => true;
    }

    [Fact]
    public async Task DuplicateMessage_WithSameCloudEventId_ShouldBeProcessedOnlyOnce()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "idempotency_test_" + Guid.NewGuid().ToString("N");
        });
        
        services.AddMongoBusConsumer<IdempotentHandler, IdempotentMessage, IdempotentDefinition>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            IdempotentHandler.HandleCount = 0;
            var cloudEventId = "fixed-id-" + Guid.NewGuid().ToString("N");

            // Wait for binding
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && await bindings.CountDocumentsAsync(x => x.Topic == "idempotent.message") == 0)
            {
                await Task.Delay(100);
            }

            // Publish first message and wait for it to be processed
            await bus.PublishAsync("idempotent.message", new IdempotentMessage { Value = "First" }, id: cloudEventId);
            
            var waitFirstTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < waitFirstTimeout && IdempotentHandler.HandleCount == 0)
            {
                await Task.Delay(100);
            }
            IdempotentHandler.HandleCount.Should().Be(1);

            // Publish second message with the same CloudEvent ID
            await bus.PublishAsync("idempotent.message", new IdempotentMessage { Value = "Second" }, id: cloudEventId);

            // Assert
            // Wait for processing
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            
            while (DateTime.UtcNow < waitTimeout)
            {
                var processedCount = await inbox.CountDocumentsAsync(x => x.Status == "Processed");
                if (processedCount >= 2) break;
                await Task.Delay(500);
            }

            IdempotentHandler.HandleCount.Should().Be(1, "The handler should only be called once because of idempotency");
            
            var processedMessages = await inbox.Find(x => x.Status == "Processed").ToListAsync();
            processedMessages.Should().HaveCount(2);
            
            var skipped = processedMessages.FirstOrDefault(x => x.LastError == "Skipped due to idempotency");
            skipped.Should().NotBeNull("One message should have been skipped");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
