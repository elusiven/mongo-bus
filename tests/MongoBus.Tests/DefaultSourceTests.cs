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
public class DefaultSourceTests(MongoDbFixture fixture)
{
    public sealed class DummyMessage { }

    public sealed class DummyHandler : IMessageHandler<DummyMessage>
    {
        public string? MyLastSource;
        public Task HandleAsync(DummyMessage message, ConsumeContext context, CancellationToken ct)
        {
            MyLastSource = context.Source;
            return Task.CompletedTask;
        }
    }

    public sealed class DummyDefinition : ConsumerDefinition<DummyHandler, DummyMessage>
    {
        public override string TypeId => "dummy.message." + Guid.NewGuid().ToString("N");
        public override string EndpointName => "dummy-endpoint." + Guid.NewGuid().ToString("N");
    }

    [Fact]
    public async Task PublishWithoutSource_ShouldUseDefaultFromOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        var dbName = "source_test_" + Guid.NewGuid().ToString("N");
        var typeId = "dummy.message." + Guid.NewGuid().ToString("N");
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
            opt.DefaultSource = "my-default-source";
        });

        // Use a dynamic definition to avoid collisions
        var definition = new DynamicDefinition(typeId, "dummy-endpoint-" + Guid.NewGuid().ToString("N"));
        services.AddSingleton<IConsumerDefinition>(definition);
        services.AddScoped<DummyHandler>();
        services.AddScoped<IMessageHandler<DummyMessage>>(sp => sp.GetRequiredService<DummyHandler>());

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Wait for binding to be registered by the runtime
            var bindings = sp.GetRequiredService<IMongoDatabase>().GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(x => x.Topic == typeId) == 0)
            {
                await Task.Delay(100);
            }

            // Act
            await bus.PublishAsync(typeId, new DummyMessage());

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            string? foundSource = null;
            while (DateTime.UtcNow < waitTimeout)
            {
                var inbox = sp.GetRequiredService<IMongoDatabase>().GetCollection<InboxMessage>("bus_inbox");
                var msg = await inbox.Find(x => x.TypeId == typeId && x.Status == "Processed").FirstOrDefaultAsync();
                if (msg != null)
                {
                    using var doc = System.Text.Json.JsonDocument.Parse(msg.PayloadJson);
                    foundSource = doc.RootElement.GetProperty("source").GetString();
                    break;
                }
                await Task.Delay(100);
            }

            foundSource.Should().Be("my-default-source");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task PublishWithExplicitSource_ShouldOverrideDefault()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        var dbName = "source_override_test_" + Guid.NewGuid().ToString("N");
        var typeId = "dummy.message." + Guid.NewGuid().ToString("N");
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
            opt.DefaultSource = "my-default-source";
        });

        var definition = new DynamicDefinition(typeId, "dummy-endpoint-" + Guid.NewGuid().ToString("N"));
        services.AddSingleton<IConsumerDefinition>(definition);
        services.AddScoped<DummyHandler>();
        services.AddScoped<IMessageHandler<DummyMessage>>(sp => sp.GetRequiredService<DummyHandler>());

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Wait for binding to be registered by the runtime
            var bindings = sp.GetRequiredService<IMongoDatabase>().GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(x => x.Topic == typeId) == 0)
            {
                await Task.Delay(100);
            }

            // Act
            await bus.PublishAsync(typeId, new DummyMessage(), source: "explicit-source");

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            string? foundSource = null;
            while (DateTime.UtcNow < waitTimeout)
            {
                var inbox = sp.GetRequiredService<IMongoDatabase>().GetCollection<InboxMessage>("bus_inbox");
                var msg = await inbox.Find(x => x.TypeId == typeId && x.Status == "Processed").FirstOrDefaultAsync();
                if (msg != null)
                {
                    using var doc = System.Text.Json.JsonDocument.Parse(msg.PayloadJson);
                    foundSource = doc.RootElement.GetProperty("source").GetString();
                    break;
                }
                await Task.Delay(100);
            }

            foundSource.Should().Be("explicit-source");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    private class DynamicDefinition(string typeId, string endpointName) : ConsumerDefinition<DummyHandler, DummyMessage>
    {
        public override string TypeId => typeId;
        public override string EndpointName => endpointName;
    }
}