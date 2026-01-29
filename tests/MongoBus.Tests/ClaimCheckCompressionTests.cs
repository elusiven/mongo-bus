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
public class ClaimCheckCompressionTests(MongoDbFixture fixture)
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
        public override string TypeId => "compressed.large.message";
    }

    [Fact]
    public async Task CompressedLargeMessage_ShouldBeStoredCompressedAndRestoredOnConsume()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "claimcheck_comp_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 128;
            opt.ClaimCheck.ProviderName = "memory";
            opt.ClaimCheck.Compression.Enabled = true;
        });

        services.AddMongoBusConsumer<LargeMessageHandler, LargeMessage, LargeMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            LargeMessageHandler.LastMessage = null;

            // Very repetitive payload - highly compressible
            var payload = new string('z', 10000);
            await bus.PublishAsync("compressed.large.message", new LargeMessage(payload));

            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && LargeMessageHandler.LastMessage == null)
            {
                await Task.Delay(100);
            }

            LargeMessageHandler.LastMessage.Should().NotBeNull();
            LargeMessageHandler.LastMessage!.Value.Should().Be(payload);

            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "compressed.large.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            var dataRef = System.Text.Json.JsonSerializer.Deserialize<ClaimCheckReference>(
                root.GetProperty("data").GetRawText());
            
            dataRef.Should().NotBeNull();
            dataRef!.Metadata.Should().ContainKey("x-mongobus-compression");
            dataRef.Metadata!["x-mongobus-compression"].Should().Be("gzip");
            
            // For 'z' repeated 10000 times, the compressed size should be significantly smaller than 10000 bytes
            // and the JSON serialization adds some overhead but not that much.
            dataRef.Length.Should().BeLessThan(1000); 
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
