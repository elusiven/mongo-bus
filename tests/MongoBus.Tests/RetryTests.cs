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
public class RetryTests(MongoDbFixture fixture)
{
    public sealed class FailingMessage { }

    public sealed class FailingHandler : IMessageHandler<FailingMessage>
    {
        public static int Attempts;
        public Task HandleAsync(FailingMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Attempts);
            throw new Exception("Boom!");
        }
    }

    public sealed class FailingDefinition : ConsumerDefinition<FailingHandler, FailingMessage>
    {
        public override string TypeId => "failing.message";
        public override int MaxAttempts => 2; // Fail once, retry once, then dead
    }

    [Fact]
    public async Task FailingHandler_ShouldRetryAndGoToDead()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "retry_test_" + Guid.NewGuid().ToString("N");
        });
        
        services.AddMongoBusConsumer<FailingHandler, FailingMessage, FailingDefinition>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            FailingHandler.Attempts = 0;

            // Act
            await bus.PublishAsync("failing.message", new FailingMessage(), "test-source");

            // Assert
            // We expect 2 attempts
            var waitTimeout = DateTime.UtcNow.AddSeconds(15);
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            
            InboxMessage? msg = null;
            while (DateTime.UtcNow < waitTimeout)
            {
                msg = await inbox.Find(x => x.Status == "Dead").FirstOrDefaultAsync();
                if (msg != null) break;
                await Task.Delay(500);
            }

            msg.Should().NotBeNull("Message should be marked as Dead");
            msg!.Attempt.Should().Be(2);
            msg.Status.Should().Be("Dead");
            msg.LastError.Should().Contain("Boom!");
            FailingHandler.Attempts.Should().Be(2);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
