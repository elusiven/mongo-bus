using System.Collections.Concurrent;
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
public class ClaimCheckTests(MongoDbFixture fixture)
{
    public sealed record LargeMessage(string Value);

    public sealed record ComplexMessage(string Name, List<int> Scores, Dictionary<string, string> Tags);

    public sealed class LargeMessageHandler : IMessageHandler<LargeMessage>
    {
        public static LargeMessage? LastMessage;
        public Task HandleAsync(LargeMessage message, ConsumeContext context, CancellationToken ct)
        {
            LastMessage = message;
            return Task.CompletedTask;
        }
    }

    public sealed class ComplexMessageHandler : IMessageHandler<ComplexMessage>
    {
        public static ComplexMessage? LastMessage;
        public Task HandleAsync(ComplexMessage message, ConsumeContext context, CancellationToken ct)
        {
            LastMessage = message;
            return Task.CompletedTask;
        }
    }

    public sealed class LargeMessageDefinition : ConsumerDefinition<LargeMessageHandler, LargeMessage>
    {
        public override string TypeId => "large.message";
    }

    public sealed class ComplexMessageDefinition : ConsumerDefinition<ComplexMessageHandler, ComplexMessage>
    {
        public override string TypeId => "complex.message";
    }

    [Fact]
    public async Task LargeMessage_ShouldUseClaimCheckAndBeRestoredOnConsume()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "claimcheck_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 128;
            opt.ClaimCheck.ProviderName = "memory";
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

            var payload = new string('x', 5000);
            await bus.PublishAsync("large.message", new LargeMessage(payload));

            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && LargeMessageHandler.LastMessage == null)
            {
                await Task.Delay(100);
            }

            LargeMessageHandler.LastMessage.Should().NotBeNull();
            LargeMessageHandler.LastMessage!.Value.Should().Be(payload);

            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "large.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            root.GetProperty("dataContentType").GetString()
                .Should().Be("application/vnd.mongobus.claim-check+json");

            var dataRef = System.Text.Json.JsonSerializer.Deserialize<ClaimCheckReference>(
                root.GetProperty("data").GetRawText());
            dataRef.Should().NotBeNull();
            dataRef!.Provider.Should().Be("memory");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ComplexMessage_ShouldUseClaimCheckAndBeRestoredOnConsume()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "claimcheck_complex_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 10; // Very low to trigger it
            opt.ClaimCheck.ProviderName = "memory";
        });

        services.AddMongoBusConsumer<ComplexMessageHandler, ComplexMessage, ComplexMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            ComplexMessageHandler.LastMessage = null;

            var message = new ComplexMessage(
                "Test Object", 
                [1, 2, 3, 4, 5], 
                new Dictionary<string, string> { { "key", "value" }, { "long", new string('a', 100) } });
            
            await bus.PublishAsync("complex.message", message);

            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && ComplexMessageHandler.LastMessage == null)
            {
                await Task.Delay(100);
            }

            ComplexMessageHandler.LastMessage.Should().NotBeNull();
            ComplexMessageHandler.LastMessage.Should().BeEquivalentTo(message);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
