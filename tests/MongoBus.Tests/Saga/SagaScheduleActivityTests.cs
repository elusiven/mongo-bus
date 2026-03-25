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
public class SagaScheduleActivityTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class StartMessage
    {
        public string OrderId { get; private set; } = "";
    }

    public sealed class TimeoutMessage
    {
        public string Reason { get; private set; } = "";
    }

    public sealed class CancelMessage
    {
        public string OrderId { get; private set; } = "";
    }

    // --- Saga State ---
    public sealed class ScheduleTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ScheduleToken { get; set; }
        public bool TimeoutReceived { get; set; }
    }

    // --- State Machine ---
    public class ScheduleStateMachine : MongoBusStateMachine<ScheduleTestState>
    {
        public SagaState WaitingForTimeout { get; private set; }
        public SagaState TimedOut { get; private set; }

        public SagaEvent<StartMessage> StartEvent { get; private set; }
        public SagaEvent<TimeoutMessage> TimeoutReceivedEvent { get; private set; }

        public SagaSchedule<ScheduleTestState, TimeoutMessage> ExpirationSchedule { get; }

        public ScheduleStateMachine()
        {
            ExpirationSchedule = new SagaSchedule<ScheduleTestState, TimeoutMessage>(
                "Expiration", "saga.test.schedule.timeout", TimeSpan.FromSeconds(2));

            Event(() => StartEvent, "saga.test.schedule.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => TimeoutReceivedEvent, "saga.test.schedule.timeout", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            ExpirationSchedule.Received = TimeoutReceivedEvent;

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .Schedule(ExpirationSchedule, ctx => new TimeoutMessage(),
                        tokenSetter: (s, t) => s.ScheduleToken = t)
                    .TransitionTo(WaitingForTimeout));

            During(WaitingForTimeout,
                When(TimeoutReceivedEvent)
                    .Then(ctx => ctx.Saga.TimeoutReceived = true)
                    .TransitionTo(TimedOut));
        }
    }

    // --- Unschedule State Machine ---
    public class UnscheduleStateMachine : MongoBusStateMachine<ScheduleTestState>
    {
        public SagaState WaitingForTimeout { get; private set; }
        public SagaState Cancelled { get; private set; }

        public SagaEvent<StartMessage> StartEvent { get; private set; }
        public SagaEvent<CancelMessage> CancelEvent { get; private set; }
        public SagaEvent<TimeoutMessage> TimeoutReceivedEvent { get; private set; }

        public SagaSchedule<ScheduleTestState, TimeoutMessage> ExpirationSchedule { get; }

        public UnscheduleStateMachine()
        {
            ExpirationSchedule = new SagaSchedule<ScheduleTestState, TimeoutMessage>(
                "Expiration", "saga.test.unsched.timeout", TimeSpan.FromSeconds(30));

            Event(() => StartEvent, "saga.test.unsched.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => CancelEvent, "saga.test.unsched.cancel", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => TimeoutReceivedEvent, "saga.test.unsched.timeout", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            ExpirationSchedule.Received = TimeoutReceivedEvent;

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .Schedule(ExpirationSchedule, ctx => new TimeoutMessage(),
                        tokenSetter: (s, t) => s.ScheduleToken = t)
                    .TransitionTo(WaitingForTimeout));

            During(WaitingForTimeout,
                When(CancelEvent)
                    .Unschedule<TimeoutMessage>((s, t) => s.ScheduleToken = t)
                    .TransitionTo(Cancelled));
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
        services.AddMongoBusSaga<ScheduleStateMachine, ScheduleTestState>();

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

    private static async Task<ScheduleTestState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<ScheduleTestState>("bus_saga_schedule-test-state");
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
    public async Task Schedule_PublishesDelayedMessage_AndHandlerReceivesIt()
    {
        var dbName = "saga_schedule_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.schedule.start",
                new StartMessage(),
                correlationId: correlationId);

            // Wait for the timeout to fire (~2 sec delay) and the saga to transition to TimedOut
            var state = await WaitForSagaStateAsync(db, correlationId, "TimedOut", timeoutSec: 15);

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
    public async Task ScheduleWithToken_StoresTokenOnInstance()
    {
        var dbName = "saga_schedule_token_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.schedule.start",
                new StartMessage(),
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "WaitingForTimeout");

            state.Should().NotBeNull();
            state!.ScheduleToken.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Unschedule_ClearsToken()
    {
        var dbName = "saga_unsched_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<UnscheduleStateMachine, ScheduleTestState>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.unsched.start",
                new StartMessage(),
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "WaitingForTimeout");
            state.Should().NotBeNull();
            state!.ScheduleToken.Should().NotBeNullOrEmpty();

            await bus.PublishAsync("saga.test.unsched.cancel",
                new CancelMessage(),
                correlationId: correlationId);

            state = await WaitForSagaStateAsync(db, correlationId, "Cancelled");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Cancelled");
            state.ScheduleToken.Should().BeNull();
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }
}
