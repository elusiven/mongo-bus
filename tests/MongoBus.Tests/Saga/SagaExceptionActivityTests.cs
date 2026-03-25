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
public class SagaExceptionActivityTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class ProcessCmd
    {
        public string Id { get; set; } = "";
        public bool ShouldFail { get; set; }
    }

    public sealed class ErrorNotification
    {
        public string ErrorMessage { get; set; } = "";
        public string CorrelationId { get; set; } = "";
    }

    // --- Saga State ---
    public sealed class ExActivityState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ErrorMessage { get; set; }
        public bool AsyncHandlerRan { get; set; }
    }

    // --- State Machine: Catch with ThenAsync ---
    public class CatchThenAsyncStateMachine : MongoBusStateMachine<ExActivityState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }
        public SagaEvent<ProcessCmd> ProcessEvent { get; private set; }

        public CatchThenAsyncStateMachine()
        {
            Event(() => ProcessEvent, "saga.exact.thenasync.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new InvalidOperationException("ThenAsync test failure");
                    })
                    .TransitionTo(Processing)
                    .CatchAll(ex => ex
                        .ThenAsync(async ctx =>
                        {
                            await Task.Delay(1);
                            ctx.Saga.AsyncHandlerRan = true;
                            ctx.Saga.ErrorMessage = ctx.Exception.Message;
                        })
                        .TransitionTo(Faulted)));
        }
    }

    // --- State Machine: Catch with TransitionTo (specific exception) ---
    public class CatchTransitionStateMachine : MongoBusStateMachine<ExActivityState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }
        public SagaEvent<ProcessCmd> ProcessEvent { get; private set; }

        public CatchTransitionStateMachine()
        {
            Event(() => ProcessEvent, "saga.exact.transition.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new ArgumentException("Transition test failure");
                    })
                    .TransitionTo(Processing)
                    .Catch<ArgumentException>(ex => ex
                        .TransitionTo(Faulted)));
        }
    }

    // --- State Machine: Catch with Finalize ---
    public class CatchFinalizeStateMachine : MongoBusStateMachine<ExActivityState>
    {
        public SagaState Processing { get; private set; }
        public SagaEvent<ProcessCmd> ProcessEvent { get; private set; }

        public CatchFinalizeStateMachine()
        {
            Event(() => ProcessEvent, "saga.exact.finalize.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new InvalidOperationException("Finalize test failure");
                    })
                    .TransitionTo(Processing)
                    .CatchAll(ex => ex.Finalize()));

            SetCompletedWhenFinalized();
        }
    }

    // --- State Machine: Catch with Publish ---
    public class CatchPublishStateMachine : MongoBusStateMachine<ExActivityState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }
        public SagaEvent<ProcessCmd> ProcessEvent { get; private set; }

        public CatchPublishStateMachine()
        {
            Event(() => ProcessEvent, "saga.exact.publish.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new InvalidOperationException("Publish test failure");
                    })
                    .TransitionTo(Processing)
                    .CatchAll(ex => ex
                        .Publish("saga.exact.error.notification",
                            ctx => new ErrorNotification
                            {
                                ErrorMessage = ctx.Exception.Message,
                                CorrelationId = ctx.Saga.CorrelationId
                            })
                        .TransitionTo(Faulted)));
        }
    }

    // --- Handler for error notification ---
    public sealed class ErrorNotificationHandler : IMessageHandler<ErrorNotification>
    {
        public static readonly List<ErrorNotification> Received = [];

        public Task HandleAsync(ErrorNotification message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class ErrorNotificationDef : ConsumerDefinition<ErrorNotificationHandler, ErrorNotification>
    {
        public override string TypeId => "saga.exact.error.notification";
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

    private static async Task<ExActivityState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<ExActivityState>("bus_saga_ex-activity-state");
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

    // --- Tests ---

    [Fact]
    public async Task Catch_ThenAsync_ExecutesAsyncHandler()
    {
        var dbName = "saga_exact_thenasync_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchThenAsyncStateMachine, ExActivityState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.exact.thenasync.process",
                new ProcessCmd { Id = "EX-A1", ShouldFail = true },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Faulted");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Faulted");
            state.AsyncHandlerRan.Should().BeTrue();
            state.ErrorMessage.Should().Be("ThenAsync test failure");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Catch_TransitionTo_TransitionsOnException()
    {
        var dbName = "saga_exact_transition_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchTransitionStateMachine, ExActivityState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.exact.transition.process",
                new ProcessCmd { Id = "EX-A2", ShouldFail = true },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Faulted");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Faulted");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Catch_Finalize_FinalizesOnException()
    {
        var dbName = "saga_exact_finalize_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchFinalizeStateMachine, ExActivityState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.exact.finalize.process",
                new ProcessCmd { Id = "EX-A3", ShouldFail = true },
                correlationId: cid);

            // SetCompletedWhenFinalized is set, so the instance should be deleted
            var collection = db.GetCollection<ExActivityState>("bus_saga_ex-activity-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            ExActivityState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state == null) break;
                await Task.Delay(100);
            }

            state.Should().BeNull("instance should be purged after Finalize with SetCompletedWhenFinalized");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Catch_Publish_PublishesOnException()
    {
        var dbName = "saga_exact_publish_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchPublishStateMachine, ExActivityState>();
        services.AddMongoBusConsumer<ErrorNotificationHandler, ErrorNotification, ErrorNotificationDef>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            ErrorNotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.exact.publish.process",
                new ProcessCmd { Id = "EX-A4", ShouldFail = true },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && ErrorNotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            ErrorNotificationHandler.Received.Should().ContainSingle();
            ErrorNotificationHandler.Received[0].ErrorMessage.Should().Be("Publish test failure");
            ErrorNotificationHandler.Received[0].CorrelationId.Should().Be(cid);

            var state = await WaitForSagaStateAsync(db, cid, "Faulted");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Faulted");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
