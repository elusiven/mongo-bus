using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class GridFsClaimCheckTests(MongoDbFixture fixture)
{
    public sealed record LargeMessage(string Value);

    public sealed class LargeMessageHandler : IMessageHandler<LargeMessage>
    {
        public static LargeMessage? LastMessage;
        public Task HandleAsync(LargeMessage message, ConsumeContext context, CancellationToken ct)
        {
            LastMessage = message;
            return Task.CompletedTask;
        }
    }

    public sealed class LargeMessageDefinition : ConsumerDefinition<LargeMessageHandler, LargeMessage>
    {
        public override string TypeId => "gridfs.large.message";
    }

    [Fact]
    public async Task LargeMessage_ShouldUseGridFsClaimCheckAndBeRestoredOnConsume()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "gridfs_claimcheck_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 128;
            opt.ClaimCheck.ProviderName = "gridfs";
        });

        services.AddMongoBusConsumer<LargeMessageHandler, LargeMessage, LargeMessageDefinition>();
        services.AddMongoBusGridFsClaimCheck();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            LargeMessageHandler.LastMessage = null;

            var payload = new string('y', 5000);
            await bus.PublishAsync("gridfs.large.message", new LargeMessage(payload));

            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && LargeMessageHandler.LastMessage == null)
            {
                await Task.Delay(100);
            }

            LargeMessageHandler.LastMessage.Should().NotBeNull();
            LargeMessageHandler.LastMessage!.Value.Should().Be(payload);

            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "gridfs.large.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            root.GetProperty("dataContentType").GetString()
                .Should().Be("application/vnd.mongobus.claim-check+json");

            var dataRef = System.Text.Json.JsonSerializer.Deserialize<ClaimCheckReference>(
                root.GetProperty("data").GetRawText());
            dataRef.Should().NotBeNull();
            dataRef!.Provider.Should().Be("gridfs");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
