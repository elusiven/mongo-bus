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
public class CorrelationTests(MongoDbFixture fixture)
{
    public record MessageA(string Value);
    public record MessageB(string Value);
    public record MessageC(string Value);

    public class HandlerA(IMessageBus bus) : IMessageHandler<MessageA>
    {
        public async Task HandleAsync(MessageA message, ConsumeContext context, CancellationToken ct)
        {
            await bus.PublishAsync("type-b", new MessageB(message.Value), ct: ct);
        }
    }

    public class HandlerB(IMessageBus bus) : IMessageHandler<MessageB>
    {
        public async Task HandleAsync(MessageB message, ConsumeContext context, CancellationToken ct)
        {
            await bus.PublishAsync("type-c", new MessageC(message.Value), ct: ct);
        }
    }

    public class HandlerC : IMessageHandler<MessageC>
    {
        public static ConsumeContext? CapturedContext;
        public Task HandleAsync(MessageC message, ConsumeContext context, CancellationToken ct)
        {
            CapturedContext = context;
            return Task.CompletedTask;
        }
    }

    public class DefinitionA : ConsumerDefinition<HandlerA, MessageA> { public override string TypeId => "type-a"; }
    public class DefinitionB : ConsumerDefinition<HandlerB, MessageB> { public override string TypeId => "type-b"; }
    public class DefinitionC : ConsumerDefinition<HandlerC, MessageC> { public override string TypeId => "type-c"; }

    [Fact]
    public async Task Correlation_ShouldPropagateAutomatically()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "correlation_test_" + Guid.NewGuid().ToString("N");
        });
        
        services.AddMongoBusConsumer<HandlerA, MessageA, DefinitionA>();
        services.AddMongoBusConsumer<HandlerB, MessageB, DefinitionB>();
        services.AddMongoBusConsumer<HandlerC, MessageC, DefinitionC>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            HandlerC.CapturedContext = null;
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var bindings = db.GetCollection<Binding>("bus_bindings");

            // Wait for bindings
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && await bindings.CountDocumentsAsync(_ => true) < 3)
            {
                await Task.Delay(100);
            }

            // Act
            var initialCorrelationId = "initial-corr-id";
            await bus.PublishAsync("type-a", new MessageA("start"), correlationId: initialCorrelationId);

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < waitTimeout && HandlerC.CapturedContext == null)
            {
                await Task.Delay(100);
            }

            HandlerC.CapturedContext.Should().NotBeNull();
            HandlerC.CapturedContext!.CorrelationId.Should().Be(initialCorrelationId);
            
            // Check CausationId of C - should be the MessageId of B
            var msgB = await inbox.Find(x => x.TypeId == "type-b").FirstOrDefaultAsync();
            msgB.Should().NotBeNull();
            HandlerC.CapturedContext.CausationId.Should().Be(msgB.Id.ToString());

            // Check CausationId of B - should be the MessageId of A
            var msgA = await inbox.Find(x => x.TypeId == "type-a").FirstOrDefaultAsync();
            msgA.Should().NotBeNull();
            msgB.CausationId.Should().Be(msgA.Id.ToString());
            msgB.CorrelationId.Should().Be(initialCorrelationId);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
