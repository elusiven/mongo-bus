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
public class SagaCompositeEventTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class PaymentReceivedMsg
    {
        public string PaymentId { get; private set; } = "";
    }

    public sealed class ShippingConfirmedMsg
    {
        public string TrackingNumber { get; private set; } = "";
    }

    // --- Saga State ---
    public sealed class CompositeTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public int EventFlags { get; set; }
        public bool AllMet { get; set; }
    }

    // --- State Machine ---
    public class CompositeStateMachine : MongoBusStateMachine<CompositeTestState>
    {
        public SagaState Waiting { get; private set; }
        public SagaState AllReady { get; private set; }

        public SagaEvent<PaymentReceivedMsg> PaymentEvent { get; private set; }
        public SagaEvent<ShippingConfirmedMsg> ShippingEvent { get; private set; }
        public SagaEvent AllRequirementsMet { get; private set; }

        public CompositeStateMachine()
        {
            Event(() => PaymentEvent, "saga.test.comp.payment", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => ShippingEvent, "saga.test.comp.shipping", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            CompositeEvent(() => AllRequirementsMet, x => x.EventFlags, PaymentEvent, ShippingEvent);

            // Register the composite behavior (side-effect via Register())
            When(AllRequirementsMet)
                .Then(s => s.AllMet = true)
                .TransitionTo(AllReady)
                .Register();

            InstanceState(x => x.CurrentState);

            Initially(
                When(PaymentEvent).TransitionTo(Waiting),
                When(ShippingEvent).TransitionTo(Waiting));

            // Both events must also be handled during Waiting so the second event is processed
            During(Waiting,
                When(PaymentEvent).Then(_ => { }),
                When(ShippingEvent).Then(_ => { }));
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
        services.AddMongoBusSaga<CompositeStateMachine, CompositeTestState>();

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

    private static async Task<CompositeTestState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<CompositeTestState>("bus_saga_composite-test-state");
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
    public async Task CompositeEvent_TriggersAfterAllRequiredEventsReceived()
    {
        var dbName = "saga_comp_all_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.comp.payment",
                new PaymentReceivedMsg(),
                correlationId: correlationId);

            await WaitForSagaStateAsync(db, correlationId, "Waiting");

            await bus.PublishAsync("saga.test.comp.shipping",
                new ShippingConfirmedMsg(),
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "AllReady");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("AllReady");
            state.AllMet.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task CompositeEvent_DoesNotTriggerBeforeAllReceived()
    {
        var dbName = "saga_comp_partial_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.comp.payment",
                new PaymentReceivedMsg(),
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Waiting");
            state.Should().NotBeNull();

            // Wait to ensure the composite event does not fire with only one event
            await Task.Delay(2000);

            var collection = db.GetCollection<CompositeTestState>("bus_saga_composite-test-state");
            var latest = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            latest!.AllMet.Should().BeFalse();
            latest.CurrentState.Should().Be("Waiting");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
