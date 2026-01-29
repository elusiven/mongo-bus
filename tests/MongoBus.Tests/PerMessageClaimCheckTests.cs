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
public class PerMessageClaimCheckTests(MongoDbFixture fixture)
{
    public sealed record SmallMessage(string Value);

    public class MessageTracker
    {
        public readonly ConcurrentDictionary<string, object> Messages = new();
    }

    public sealed class SmallMessageHandler(MessageTracker tracker) : IMessageHandler<SmallMessage>
    {
        public Task HandleAsync(SmallMessage message, ConsumeContext context, CancellationToken ct)
        {
            tracker.Messages[context.CloudEventId] = message;
            return Task.CompletedTask;
        }
    }

    public sealed class SmallMessageDefinition : ConsumerDefinition<SmallMessageHandler, SmallMessage>
    {
        public override string TypeId => "small.message";
    }

    [Fact]
    public async Task SmallMessage_ShouldBeClaimChecked_WhenForcedPerMessage()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = ("db" + Guid.NewGuid().ToString("N"))[..30];
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 10000; // High threshold
            opt.ClaimCheck.ProviderName = "memory";
        });

        services.AddMongoBusConsumer<SmallMessageHandler, SmallMessage, SmallMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();
        services.AddSingleton<MessageTracker>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();
        var tracker = sp.GetRequiredService<MessageTracker>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var payload = "Small but forced";
            var messageId = Guid.NewGuid().ToString("N");

            // Ensure bindings are created before publishing
            var waitTimeout = DateTime.UtcNow.AddSeconds(5);
            var bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
            while (DateTime.UtcNow < waitTimeout && !await bindings.Find(x => x.Topic == "small.message").AnyAsync())
            {
                await Task.Delay(100);
            }

            await bus.PublishAsync("small.message", new SmallMessage(payload), id: messageId, useClaimCheck: true);

            waitTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < waitTimeout && !tracker.Messages.ContainsKey(messageId))
            {
                await Task.Delay(100);
            }

            tracker.Messages.TryGetValue(messageId, out var received).Should().BeTrue();
            ((SmallMessage)received!).Value.Should().Be(payload);

            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "small.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            root.GetProperty("dataContentType").GetString()
                .Should().Be("application/vnd.mongobus.claim-check+json");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task LargeMessage_ShouldBeClaimChecked_WhenDisabledPerMessageButGloballyEnabled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = ("db" + Guid.NewGuid().ToString("N"))[..30];
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 10; // Low threshold
            opt.ClaimCheck.ProviderName = "memory";
        });

        services.AddMongoBusConsumer<SmallMessageHandler, SmallMessage, SmallMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();
        services.AddSingleton<MessageTracker>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();
        var tracker = sp.GetRequiredService<MessageTracker>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var payload = new string('x', 500); // Larger than threshold
            var messageId = Guid.NewGuid().ToString("N");

            // Ensure bindings are created before publishing
            var waitTimeout = DateTime.UtcNow.AddSeconds(5);
            var bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
            while (DateTime.UtcNow < waitTimeout && !await bindings.Find(x => x.Topic == "small.message").AnyAsync())
            {
                await Task.Delay(100);
            }

            // Even if we say useClaimCheck: false, it should be claim-checked because it's large and globally enabled.
            await bus.PublishAsync("small.message", new SmallMessage(payload), id: messageId, useClaimCheck: false);

            waitTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < waitTimeout && !tracker.Messages.ContainsKey(messageId))
            {
                await Task.Delay(100);
            }

            tracker.Messages.TryGetValue(messageId, out var received).Should().BeTrue();
            ((SmallMessage)received!).Value.Should().Be(payload);

            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "small.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            root.GetProperty("dataContentType").GetString()
                .Should().Be("application/vnd.mongobus.claim-check+json");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task LargeMessage_ShouldBeClaimChecked_WhenGloballyDisabledButForcedPerMessage()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = ("db" + Guid.NewGuid().ToString("N"))[..30];
            opt.ClaimCheck.Enabled = false; // Globally disabled
            opt.ClaimCheck.ThresholdBytes = 10;
            opt.ClaimCheck.ProviderName = "memory";
        });

        services.AddMongoBusConsumer<SmallMessageHandler, SmallMessage, SmallMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();
        services.AddSingleton<MessageTracker>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();
        var tracker = sp.GetRequiredService<MessageTracker>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var payload = new string('z', 500);
            var messageId = Guid.NewGuid().ToString("N");

            // Ensure bindings are created before publishing
            var waitTimeout = DateTime.UtcNow.AddSeconds(5);
            var bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
            while (DateTime.UtcNow < waitTimeout && !await bindings.Find(x => x.Topic == "small.message").AnyAsync())
            {
                await Task.Delay(100);
            }

            await bus.PublishAsync("small.message", new SmallMessage(payload), id: messageId, useClaimCheck: true);

            waitTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < waitTimeout && !tracker.Messages.ContainsKey(messageId))
            {
                await Task.Delay(100);
            }

            tracker.Messages.TryGetValue(messageId, out var received).Should().BeTrue();
            
            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            var msg = await inbox.Find(x => x.TypeId == "small.message").FirstOrDefaultAsync();
            msg.Should().NotBeNull();

            using var doc = System.Text.Json.JsonDocument.Parse(msg!.PayloadJson);
            var root = doc.RootElement;
            root.GetProperty("dataContentType").GetString()
                .Should().Be("application/vnd.mongobus.claim-check+json");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
