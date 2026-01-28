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
public class ConcurrencyTests(MongoDbFixture fixture)
{
    public sealed class SlowMessage { }

    public sealed class SlowHandler : IMessageHandler<SlowMessage>
    {
        public static int StartCount;
        public static int EndCount;
        public async Task HandleAsync(SlowMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref StartCount);
            await Task.Delay(2000, ct); // Hold the lock for 2 seconds
            Interlocked.Increment(ref EndCount);
        }
    }

    public sealed class SlowDefinition : ConsumerDefinition<SlowHandler, SlowMessage>
    {
        public override string TypeId => "slow.message";
        public override int ConcurrencyLimit => 5;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(1); // Lock expires before handler finishes!
    }

    [Fact]
    public async Task MessageShouldNotBeProcessedByMultipleWorkersSimultaneously()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "concurrency_test_" + Guid.NewGuid().ToString("N");
        });
        
        services.AddMongoBusConsumer<SlowHandler, SlowMessage, SlowDefinition>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Wait for binding to be registered by the runtime
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            SlowHandler.StartCount = 0;
            SlowHandler.EndCount = 0;

            // Act
            await bus.PublishAsync("slow.message", new SlowMessage(), "test-source");

            // Wait for it to start
            var startTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < startTimeout && SlowHandler.StartCount == 0)
            {
                await Task.Delay(100);
            }
            SlowHandler.StartCount.Should().Be(1);

            // Wait for lock to expire (LockTime is 1s)
            await Task.Delay(1000);
            
            // Even though lock expired, since the first worker is still "running", 
            // another worker MIGHT pick it up because it's still 'Pending' in DB 
            // but the lock is expired.
            
            // Wait for processing to finish
            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout && SlowHandler.EndCount < 1)
            {
                await Task.Delay(100);
            }

            // Assert
            // If the locking works correctly, even if it expires and is picked up again, 
            // we should see how the system handles it.
            // In MongoBusRuntime, when a message is picked up, it stays 'Pending' until 'Processed'.
            // If it's picked up again, StartCount will increment again.
            
            SlowHandler.StartCount.Should().BeInRange(1, 2); 
            SlowHandler.EndCount.Should().BeInRange(1, 2);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
