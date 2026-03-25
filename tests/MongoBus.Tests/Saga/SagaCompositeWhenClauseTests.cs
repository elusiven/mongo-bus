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
public class SagaCompositeWhenClauseTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class PartAMsg
    {
        public string Value { get; set; } = "";
    }

    public sealed class PartBMsg
    {
        public string Value { get; set; } = "";
    }

    public sealed class CompositeNotification
    {
        public string Summary { get; set; } = "";
    }

    // --- Saga State ---
    public sealed class CompClauseState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public int EventFlags { get; set; }
        public bool AsyncActionRan { get; set; }
        public string? AsyncResult { get; set; }
    }

    // --- State Machine: CompositeWhen with ThenAsync ---
    public class CompositeThenAsyncStateMachine : MongoBusStateMachine<CompClauseState>
    {
        public SagaState Waiting { get; private set; }
        public SagaState Done { get; private set; }

        public SagaEvent<PartAMsg> PartAEvent { get; private set; }
        public SagaEvent<PartBMsg> PartBEvent { get; private set; }
        public SagaEvent AllReady { get; private set; }

        public CompositeThenAsyncStateMachine()
        {
            Event(() => PartAEvent, "saga.compclause.thenasync.parta", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => PartBEvent, "saga.compclause.thenasync.partb", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            CompositeEvent(() => AllReady, x => x.EventFlags, PartAEvent, PartBEvent);

            When(AllReady)
                .ThenAsync(async s =>
                {
                    await Task.Delay(1);
                    s.AsyncActionRan = true;
                    s.AsyncResult = "async-composite-done";
                })
                .TransitionTo(Done)
                .Register();

            InstanceState(x => x.CurrentState);

            Initially(
                When(PartAEvent).TransitionTo(Waiting),
                When(PartBEvent).TransitionTo(Waiting));

            During(Waiting,
                When(PartAEvent).Then(_ => { }),
                When(PartBEvent).Then(_ => { }));
        }
    }

    // --- State Machine: CompositeWhen with Publish ---
    public class CompositePublishStateMachine : MongoBusStateMachine<CompClauseState>
    {
        public SagaState Waiting { get; private set; }
        public SagaState Done { get; private set; }

        public SagaEvent<PartAMsg> PartAEvent { get; private set; }
        public SagaEvent<PartBMsg> PartBEvent { get; private set; }
        public SagaEvent AllReady { get; private set; }

        public CompositePublishStateMachine()
        {
            Event(() => PartAEvent, "saga.compclause.publish.parta", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => PartBEvent, "saga.compclause.publish.partb", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            CompositeEvent(() => AllReady, x => x.EventFlags, PartAEvent, PartBEvent);

            When(AllReady)
                .Publish("saga.compclause.notification",
                    s => new CompositeNotification { Summary = "all-parts-ready" })
                .TransitionTo(Done)
                .Register();

            InstanceState(x => x.CurrentState);

            Initially(
                When(PartAEvent).TransitionTo(Waiting),
                When(PartBEvent).TransitionTo(Waiting));

            During(Waiting,
                When(PartAEvent).Then(_ => { }),
                When(PartBEvent).Then(_ => { }));
        }
    }

    // --- State Machine: CompositeWhen with Finalize ---
    public class CompositeFinalizeStateMachine : MongoBusStateMachine<CompClauseState>
    {
        public SagaState Waiting { get; private set; }

        public SagaEvent<PartAMsg> PartAEvent { get; private set; }
        public SagaEvent<PartBMsg> PartBEvent { get; private set; }
        public SagaEvent AllReady { get; private set; }

        public CompositeFinalizeStateMachine()
        {
            Event(() => PartAEvent, "saga.compclause.finalize.parta", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => PartBEvent, "saga.compclause.finalize.partb", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            CompositeEvent(() => AllReady, x => x.EventFlags, PartAEvent, PartBEvent);

            When(AllReady)
                .Finalize()
                .Register();

            InstanceState(x => x.CurrentState);

            Initially(
                When(PartAEvent).TransitionTo(Waiting),
                When(PartBEvent).TransitionTo(Waiting));

            During(Waiting,
                When(PartAEvent).Then(_ => { }),
                When(PartBEvent).Then(_ => { }));

            SetCompletedWhenFinalized();
        }
    }

    // --- Handler for composite publish notification ---
    public sealed class CompositeNotificationHandler : IMessageHandler<CompositeNotification>
    {
        public static readonly List<CompositeNotification> Received = [];

        public Task HandleAsync(CompositeNotification message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class CompositeNotificationDef : ConsumerDefinition<CompositeNotificationHandler, CompositeNotification>
    {
        public override string TypeId => "saga.compclause.notification";
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

    private static async Task<CompClauseState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<CompClauseState>("bus_saga_comp-clause-state");
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
    public async Task CompositeWhen_ThenAsync_ExecutesAsyncAction()
    {
        var dbName = "saga_compclause_thenasync_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CompositeThenAsyncStateMachine, CompClauseState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.compclause.thenasync.parta",
                new PartAMsg { Value = "A" },
                correlationId: correlationId);

            await WaitForSagaStateAsync(db, correlationId, "Waiting");

            await bus.PublishAsync("saga.compclause.thenasync.partb",
                new PartBMsg { Value = "B" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Done");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Done");
            state.AsyncActionRan.Should().BeTrue();
            state.AsyncResult.Should().Be("async-composite-done");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task CompositeWhen_Publish_PublishesMessage()
    {
        var dbName = "saga_compclause_publish_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CompositePublishStateMachine, CompClauseState>();
        services.AddMongoBusConsumer<CompositeNotificationHandler, CompositeNotification, CompositeNotificationDef>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);
            CompositeNotificationHandler.Received.Clear();

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.compclause.publish.parta",
                new PartAMsg { Value = "A" },
                correlationId: correlationId);

            await WaitForSagaStateAsync(db, correlationId, "Waiting");

            await bus.PublishAsync("saga.compclause.publish.partb",
                new PartBMsg { Value = "B" },
                correlationId: correlationId);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && CompositeNotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            CompositeNotificationHandler.Received.Should().ContainSingle();
            CompositeNotificationHandler.Received[0].Summary.Should().Be("all-parts-ready");

            var state = await WaitForSagaStateAsync(db, correlationId, "Done");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Done");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task CompositeWhen_Finalize_DeletesInstance()
    {
        var dbName = "saga_compclause_finalize_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CompositeFinalizeStateMachine, CompClauseState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.compclause.finalize.parta",
                new PartAMsg { Value = "A" },
                correlationId: correlationId);

            await WaitForSagaStateAsync(db, correlationId, "Waiting");

            await bus.PublishAsync("saga.compclause.finalize.partb",
                new PartBMsg { Value = "B" },
                correlationId: correlationId);

            // SetCompletedWhenFinalized is set, so the instance should be deleted
            var collection = db.GetCollection<CompClauseState>("bus_saga_comp-clause-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            CompClauseState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (state == null) break;
                await Task.Delay(100);
            }

            state.Should().BeNull("instance should be purged after composite Finalize with SetCompletedWhenFinalized");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
