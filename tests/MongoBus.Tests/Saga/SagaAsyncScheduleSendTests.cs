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
public class SagaAsyncScheduleSendTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class StartMsg
    {
        public string OrderId { get; set; } = "";
    }

    public sealed class TimeoutMsg
    {
        public string Reason { get; set; } = "";
    }

    public sealed class NotificationMsg
    {
        public string Content { get; set; } = "";
    }

    // --- Saga States ---
    public sealed class AsyncScheduleState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? Token { get; set; }
        public bool TimeoutReceived { get; set; }
    }

    public sealed class AsyncSendState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    // --- State Machines ---
    public class AsyncScheduleStateMachine : MongoBusStateMachine<AsyncScheduleState>
    {
        public SagaState Processing { get; private set; }
        public SagaState TimedOut { get; private set; }

        public SagaEvent<StartMsg> StartEvent { get; private set; }
        public SagaEvent<TimeoutMsg> TimeoutReceivedEvent { get; private set; }

        public SagaSchedule<AsyncScheduleState, TimeoutMsg> ExpirationSchedule { get; }

        public AsyncScheduleStateMachine()
        {
            ExpirationSchedule = new SagaSchedule<AsyncScheduleState, TimeoutMsg>(
                "Expiration", "saga.asyncsched.timeout", TimeSpan.FromSeconds(2));

            Event(() => StartEvent, "saga.asyncsched.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => TimeoutReceivedEvent, "saga.asyncsched.timeout", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            ExpirationSchedule.Received = TimeoutReceivedEvent;

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .ScheduleAsync(ExpirationSchedule, async ctx =>
                    {
                        await Task.Delay(1);
                        return new TimeoutMsg { Reason = "async-timeout" };
                    })
                    .TransitionTo(Processing));

            During(Processing,
                When(TimeoutReceivedEvent)
                    .Then(ctx => ctx.Saga.TimeoutReceived = true)
                    .TransitionTo(TimedOut));
        }
    }

    public class AsyncScheduleWithTokenStateMachine : MongoBusStateMachine<AsyncScheduleState>
    {
        public SagaState Processing { get; private set; }
        public SagaState TimedOut { get; private set; }

        public SagaEvent<StartMsg> StartEvent { get; private set; }
        public SagaEvent<TimeoutMsg> TimeoutReceivedEvent { get; private set; }

        public SagaSchedule<AsyncScheduleState, TimeoutMsg> ExpirationSchedule { get; }

        public AsyncScheduleWithTokenStateMachine()
        {
            ExpirationSchedule = new SagaSchedule<AsyncScheduleState, TimeoutMsg>(
                "Expiration", "saga.asyncschedtoken.timeout", TimeSpan.FromSeconds(2));

            Event(() => StartEvent, "saga.asyncschedtoken.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => TimeoutReceivedEvent, "saga.asyncschedtoken.timeout", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            ExpirationSchedule.Received = TimeoutReceivedEvent;

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .ScheduleAsync(ExpirationSchedule, async ctx =>
                    {
                        await Task.Delay(1);
                        return new TimeoutMsg { Reason = "async-token-timeout" };
                    }, tokenSetter: (s, t) => s.Token = t)
                    .TransitionTo(Processing));

            During(Processing,
                When(TimeoutReceivedEvent)
                    .Then(ctx => ctx.Saga.TimeoutReceived = true)
                    .TransitionTo(TimedOut));
        }
    }

    public class AsyncSendStateMachine : MongoBusStateMachine<AsyncSendState>
    {
        public SagaState Sent { get; private set; }
        public SagaEvent<StartMsg> StartEvent { get; private set; }

        public AsyncSendStateMachine()
        {
            Event(() => StartEvent, "saga.asyncsend.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .SendAsync("target-async-ep", "saga.asyncsend.notification", async ctx =>
                    {
                        await Task.Delay(1);
                        return new NotificationMsg { Content = ctx.Message.OrderId };
                    })
                    .TransitionTo(Sent));
        }
    }

    // --- Handler for async send ---
    public sealed class AsyncNotificationHandler : IMessageHandler<NotificationMsg>
    {
        public static readonly List<(NotificationMsg Message, ConsumeContext Context)> Received = [];

        public Task HandleAsync(NotificationMsg message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add((message, context));
            return Task.CompletedTask;
        }
    }

    public sealed class AsyncNotificationDef : ConsumerDefinition<AsyncNotificationHandler, NotificationMsg>
    {
        public override string TypeId => "saga.asyncsend.notification";
        public override string EndpointName => "target-async-ep";
    }

    // --- Helpers ---
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

    private static async Task<T?> WaitForSagaStateAsync<T>(
        IMongoDatabase db,
        string collectionName,
        string correlationId,
        string expectedState,
        int timeoutSec = 10) where T : class, ISagaInstance
    {
        var collection = db.GetCollection<T>(collectionName);
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < timeout)
        {
            var instance = await collection
                .Find(Builders<T>.Filter.Eq("CorrelationId", correlationId))
                .FirstOrDefaultAsync();

            if (instance?.CurrentState == expectedState)
                return instance;

            await Task.Delay(100);
        }

        return await collection
            .Find(Builders<T>.Filter.Eq("CorrelationId", correlationId))
            .FirstOrDefaultAsync();
    }

    // --- Tests ---

    [Fact]
    public async Task ScheduleAsync_PublishesDelayedMessage()
    {
        var dbName = "saga_asyncsched_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<AsyncScheduleStateMachine, AsyncScheduleState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.asyncsched.start",
                new StartMsg { OrderId = "AS-1" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync<AsyncScheduleState>(
                db, "bus_saga_async-schedule-state", correlationId, "TimedOut", timeoutSec: 15);

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("TimedOut");
            state.TimeoutReceived.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task ScheduleWithTokenAsync_StoresToken()
    {
        var dbName = "saga_asyncschedtok_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<AsyncScheduleWithTokenStateMachine, AsyncScheduleState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.asyncschedtoken.start",
                new StartMsg { OrderId = "AS-2" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync<AsyncScheduleState>(
                db, "bus_saga_async-schedule-state", correlationId, "Processing");

            state.Should().NotBeNull();
            state!.Token.Should().NotBeNullOrEmpty();

            // Also verify the timeout fires and is handled
            var finalState = await WaitForSagaStateAsync<AsyncScheduleState>(
                db, "bus_saga_async-schedule-state", correlationId, "TimedOut", timeoutSec: 15);

            finalState.Should().NotBeNull();
            finalState!.CurrentState.Should().Be("TimedOut");
            finalState.TimeoutReceived.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task SendAsync_PublishesMessageToTargetEndpoint()
    {
        var dbName = "saga_asyncsend_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<AsyncSendStateMachine, AsyncSendState>();
        services.AddMongoBusConsumer<AsyncNotificationHandler, NotificationMsg, AsyncNotificationDef>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            AsyncNotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.asyncsend.start",
                new StartMsg { OrderId = "ASYNC-SEND-1" },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && AsyncNotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            AsyncNotificationHandler.Received.Should().ContainSingle();
            AsyncNotificationHandler.Received[0].Message.Content.Should().Be("ASYNC-SEND-1");

            var state = await WaitForSagaStateAsync<AsyncSendState>(
                db, "bus_saga_async-send-state", cid, "Sent");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Sent");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
