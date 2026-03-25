using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaPublishActivityTests(MongoDbFixture fixture)
{
    public sealed class StartOrder { public string OrderId { get; set; } = ""; }
    public sealed class OrderNotification { public string OrderId { get; set; } = ""; public string Status { get; set; } = ""; }

    public sealed class PublishTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public sealed class NotificationHandler : IMessageHandler<OrderNotification>
    {
        public static readonly List<OrderNotification> Received = [];
        public Task HandleAsync(OrderNotification message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class NotificationDef : ConsumerDefinition<NotificationHandler, OrderNotification>
    {
        public override string TypeId => "saga.pub.notification";
    }

    public class PublishStateMachine : MongoBusStateMachine<PublishTestState>
    {
        public SagaState Submitted { get; private set; }
        public SagaEvent<StartOrder> OrderStarted { get; private set; }

        public PublishStateMachine()
        {
            Event(() => OrderStarted, "saga.pub.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderStarted)
                    .Publish("saga.pub.notification",
                        ctx => new OrderNotification { OrderId = ctx.Message.OrderId, Status = "submitted" })
                    .TransitionTo(Submitted));
        }
    }

    [Fact]
    public async Task Publish_SendsMessageViaMessageBus()
    {
        var dbName = "saga_pub_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<PublishStateMachine, PublishTestState>();
        services.AddMongoBusConsumer<NotificationHandler, OrderNotification, NotificationDef>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            NotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.pub.start",
                new StartOrder { OrderId = "PUB-1" },
                correlationId: cid);

            // Wait for the notification to be consumed
            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && NotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            NotificationHandler.Received.Should().ContainSingle();
            NotificationHandler.Received[0].OrderId.Should().Be("PUB-1");
            NotificationHandler.Received[0].Status.Should().Be("submitted");

            // Verify saga state also transitioned
            var collection = db.GetCollection<PublishTestState>("bus_saga_publish-test-state");
            var state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Submitted");
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Publish_PropagatesCorrelationId()
    {
        var dbName = "saga_pub_corr_" + Guid.NewGuid().ToString("N");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<PublishStateMachine, PublishTestState>();
        services.AddMongoBusConsumer<NotificationHandler, OrderNotification, NotificationDef>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            NotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.pub.start",
                new StartOrder { OrderId = "PUB-2" },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && NotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var notification = await inbox.Find(x => x.TypeId == "saga.pub.notification" && x.CorrelationId == cid)
                .FirstOrDefaultAsync();

            notification.Should().NotBeNull();
            notification!.CorrelationId.Should().Be(cid);
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }
}
