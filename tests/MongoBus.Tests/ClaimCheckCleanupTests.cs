using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class ClaimCheckCleanupTests(MongoDbFixture fixture)
{
    public sealed record LargeMessage(string Value);

    public sealed class LargeMessageHandler : IMessageHandler<LargeMessage>
    {
        public Task HandleAsync(LargeMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class LargeMessageDefinition : ConsumerDefinition<LargeMessageHandler, LargeMessage>
    {
        public override string TypeId => "cleanup.large.message";
    }

    [Fact]
    public async Task CleanupService_ShouldDeleteOrphanedClaimChecks()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "cleanup_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 10;
            opt.ClaimCheck.ProviderName = "memory";
            opt.ClaimCheck.Cleanup.Interval = TimeSpan.FromMilliseconds(500);
            opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.Zero; // Clean immediately for test
        });

        services.AddMongoBusConsumer<LargeMessageHandler, LargeMessage, LargeMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();
        var provider = sp.GetRequiredService<IClaimCheckProvider>() as InMemoryClaimCheckProvider;

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // 1. Publish a message that uses claim-check
            await bus.PublishAsync("cleanup.large.message", new LargeMessage("Large enough payload"));

            // Wait for it to be stored
            await Task.Delay(500);

            // Verify it exists in memory provider
            var refsList = new List<ClaimCheckReference>();
            await foreach (var r in provider!.ListAsync(default)) refsList.Add(r);
            refsList.Should().HaveCount(1);
            var key = refsList[0].Key;

            // 2. Manually delete the InboxMessage to simulate TTL or manual deletion
            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            await inbox.DeleteManyAsync(_ => true);

            // 3. Wait for cleanup service to run
            await Task.Delay(2000);

            // 4. Verify claim-check is gone from provider
            refsList.Clear();
            await foreach (var r in provider!.ListAsync(default)) refsList.Add(r);
            refsList.Should().BeEmpty();
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
