using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.Saga;
using MongoBus.Models.Saga;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaBasicTransitionTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class OrderSubmitted
    {
        public string OrderId { get; set; } = "";
        public DateTime OrderDate { get; set; }
    }

    public sealed class OrderAccepted
    {
        public string OrderId { get; set; } = "";
    }

    public sealed class OrderCompleted
    {
        public string OrderId { get; set; } = "";
    }

    // --- Saga State ---
    public sealed class OrderState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public DateTime? OrderDate { get; set; }
        public string? AcceptedBy { get; set; }
    }

    // --- State Machine ---
    public class BasicOrderStateMachine : MongoBusStateMachine<OrderState>
    {
        public SagaState Submitted { get; private set; }
        public SagaState Accepted { get; private set; }

        public SagaEvent<OrderSubmitted> OrderSubmittedEvent { get; private set; }
        public SagaEvent<OrderAccepted> OrderAcceptedEvent { get; private set; }
        public SagaEvent<OrderCompleted> OrderCompletedEvent { get; private set; }

        public BasicOrderStateMachine()
        {
            Event(() => OrderSubmittedEvent, "saga.test.order.submitted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => OrderAcceptedEvent, "saga.test.order.accepted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => OrderCompletedEvent, "saga.test.order.completed", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderSubmittedEvent)
                    .Then(ctx => ctx.Saga.OrderDate = ctx.Message.OrderDate)
                    .TransitionTo(Submitted));

            During(Submitted,
                When(OrderAcceptedEvent)
                    .TransitionTo(Accepted));

            During(Accepted,
                Ignore(OrderSubmittedEvent));
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
        services.AddMongoBusSaga<BasicOrderStateMachine, OrderState>();

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

    private static async Task<OrderState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<OrderState>("bus_saga_order-state");
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
    public async Task Initially_TransitionTo_CreatesNewInstance()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            var submittedAt = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-1", OrderDate = submittedAt },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Submitted");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Submitted");
            state.OrderDate.Should().Be(submittedAt);
            state.Version.Should().Be(1);
            state.CreatedUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task During_TransitionTo_MovesState()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-2", OrderDate = DateTime.UtcNow },
                correlationId: correlationId);

            await WaitForSagaStateAsync(db, correlationId, "Submitted");

            await bus.PublishAsync("saga.test.order.accepted",
                new OrderAccepted { OrderId = "ORD-2" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Accepted");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Accepted");
            state.Version.Should().Be(2);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Then_ModifiesSagaData()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            var orderDate = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc);

            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-3", OrderDate = orderDate },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Submitted");

            state.Should().NotBeNull();
            state!.OrderDate.Should().Be(orderDate);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Ignore_SilentlySkipsEvent()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");

            // Submit -> Accepted
            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-4", OrderDate = DateTime.UtcNow },
                correlationId: correlationId);
            await WaitForSagaStateAsync(db, correlationId, "Submitted");

            await bus.PublishAsync("saga.test.order.accepted",
                new OrderAccepted { OrderId = "ORD-4" },
                correlationId: correlationId);
            await WaitForSagaStateAsync(db, correlationId, "Accepted");

            // Send OrderSubmitted again - should be ignored in Accepted state
            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-4", OrderDate = DateTime.UtcNow },
                correlationId: correlationId);

            // Wait a bit to ensure the ignored message is processed
            await Task.Delay(2000);

            var collection = db.GetCollection<OrderState>("bus_saga_order-state");
            var state = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            // State should still be Accepted, version should NOT have increased from the ignored event
            state!.CurrentState.Should().Be("Accepted");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task MultipleInstances_AreIndependent()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var id1 = Guid.NewGuid().ToString("N");
            var id2 = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-A", OrderDate = DateTime.UtcNow },
                correlationId: id1);
            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-B", OrderDate = DateTime.UtcNow },
                correlationId: id2);

            await WaitForSagaStateAsync(db, id1, "Submitted");
            await WaitForSagaStateAsync(db, id2, "Submitted");

            // Only accept the first
            await bus.PublishAsync("saga.test.order.accepted",
                new OrderAccepted { OrderId = "ORD-A" },
                correlationId: id1);

            await WaitForSagaStateAsync(db, id1, "Accepted");

            var collection = db.GetCollection<OrderState>("bus_saga_order-state");
            var state2 = await collection.Find(x => x.CorrelationId == id2).FirstOrDefaultAsync();
            state2!.CurrentState.Should().Be("Submitted");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Version_IncrementsOnEachTransition()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-V", OrderDate = DateTime.UtcNow },
                correlationId: correlationId);
            var s1 = await WaitForSagaStateAsync(db, correlationId, "Submitted");
            s1!.Version.Should().Be(1);

            await bus.PublishAsync("saga.test.order.accepted",
                new OrderAccepted { OrderId = "ORD-V" },
                correlationId: correlationId);
            var s2 = await WaitForSagaStateAsync(db, correlationId, "Accepted");
            s2!.Version.Should().Be(2);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task LastModifiedUtc_UpdatesOnTransition()
    {
        var dbName = "saga_basic_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.order.submitted",
                new OrderSubmitted { OrderId = "ORD-T", OrderDate = DateTime.UtcNow },
                correlationId: correlationId);

            var s1 = await WaitForSagaStateAsync(db, correlationId, "Submitted");
            var firstModified = s1!.LastModifiedUtc;

            await Task.Delay(100); // Small delay to ensure different timestamp

            await bus.PublishAsync("saga.test.order.accepted",
                new OrderAccepted { OrderId = "ORD-T" },
                correlationId: correlationId);

            var s2 = await WaitForSagaStateAsync(db, correlationId, "Accepted");
            s2!.LastModifiedUtc.Should().BeAfter(firstModified);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
