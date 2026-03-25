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
public class SagaSendActivityTests(MongoDbFixture fixture)
{
    public sealed class StartSendMessage
    {
        public string Message { get; set; } = "";
    }

    public sealed class SendNotification
    {
        public string Message { get; set; } = "";
    }

    public sealed class SendTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public sealed class NotificationHandler : IMessageHandler<SendNotification>
    {
        public static readonly List<(SendNotification Message, ConsumeContext Context)> Received = [];

        public Task HandleAsync(SendNotification message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add((message, context));
            return Task.CompletedTask;
        }
    }

    public sealed class NotificationDef : ConsumerDefinition<NotificationHandler, SendNotification>
    {
        public override string TypeId => "saga.test.send.notification";
        public override string EndpointName => "target-endpoint";
    }

    public class SendTestStateMachine : MongoBusStateMachine<SendTestState>
    {
        public SagaState Sent { get; private set; }
        public SagaEvent<StartSendMessage> StartSendEvent { get; private set; }

        public SendTestStateMachine()
        {
            Event(() => StartSendEvent, "saga.test.send.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartSendEvent)
                    .Send("target-endpoint", "saga.test.send.notification",
                        ctx => new SendNotification { Message = ctx.Message.Message })
                    .TransitionTo(Sent));
        }
    }

    private (ServiceProvider sp, IMessageBus bus, IMongoDatabase db) BuildAndStart(string dbName)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<SendTestStateMachine, SendTestState>();
        services.AddMongoBusConsumer<NotificationHandler, SendNotification, NotificationDef>();

        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredService<IMessageBus>(), sp.GetRequiredService<IMongoDatabase>());
    }

    private static async Task<List<IHostedService>> StartAsync(ServiceProvider sp)
    {
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopAsync(IEnumerable<IHostedService> services)
    {
        foreach (var hs in services) await hs.StopAsync(CancellationToken.None);
    }

    private static async Task WaitForBindingsAsync(IMongoDatabase db, int expectedCount = 1, int timeoutSec = 5)
    {
        var bindings = db.GetCollection<Binding>("bus_bindings");
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < timeout &&
               await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < expectedCount)
        {
            await Task.Delay(100);
        }
    }

    private static async Task<SendTestState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<SendTestState>("bus_saga_send-test-state");
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < timeout)
        {
            var instance = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            if (instance?.CurrentState == expectedState)
                return instance;

            await Task.Delay(100);
        }

        return await collection
            .Find(x => x.CorrelationId == correlationId)
            .FirstOrDefaultAsync();
    }

    [Fact]
    public async Task Send_PublishesMessageToTargetEndpoint()
    {
        var dbName = "saga_send_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            NotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.send.start",
                new StartSendMessage { Message = "Hello from saga" },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && NotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            NotificationHandler.Received.Should().ContainSingle();
            NotificationHandler.Received[0].Message.Message.Should().Be("Hello from saga");

            var state = await WaitForSagaStateAsync(db, cid, "Sent");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Sent");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Send_PropagatesCorrelationId()
    {
        var dbName = "saga_send_corr_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            NotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.send.start",
                new StartSendMessage { Message = "Correlation test" },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && NotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var notification = await inbox
                .Find(x => x.TypeId == "saga.test.send.notification" && x.CorrelationId == cid)
                .FirstOrDefaultAsync();

            notification.Should().NotBeNull();
            notification!.CorrelationId.Should().Be(cid);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
