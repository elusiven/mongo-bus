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
public class SagaCorrelateByTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class CreateOrder
    {
        public string OrderNumber { get; private set; } = "";
    }

    public sealed class UpdateOrder
    {
        public string Note { get; private set; } = "";
    }

    // --- Saga State ---
    public sealed class CorrelateByState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string OrderNumber { get; set; } = "";
        public bool WasUpdated { get; set; }
    }

    // --- State Machine ---
    // Uses CausationId to pass the OrderNumber from the test into the saga instance,
    // since message properties have private setters.
    public class CorrelateByStateMachine : MongoBusStateMachine<CorrelateByState>
    {
        public SagaState Created { get; private set; }
        public SagaState Updated { get; private set; }

        public SagaEvent<CreateOrder> CreateOrderEvent { get; private set; }
        public SagaEvent<UpdateOrder> UpdateOrderEvent { get; private set; }

        public CorrelateByStateMachine()
        {
            Event(() => CreateOrderEvent, "saga.test.corr.create", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => UpdateOrderEvent, "saga.test.corr.update", e =>
                e.CorrelateBy(x => x.OrderNumber, ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(CreateOrderEvent)
                    .Then(ctx => ctx.Saga.OrderNumber = ctx.Context.CausationId!)
                    .TransitionTo(Created));

            During(Created,
                When(UpdateOrderEvent)
                    .Then(ctx => ctx.Saga.WasUpdated = true)
                    .TransitionTo(Updated));
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
        services.AddMongoBusSaga<CorrelateByStateMachine, CorrelateByState>();

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

    private static async Task<CorrelateByState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<CorrelateByState>("bus_saga_correlate-by-state");
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
    public async Task CorrelateBy_FindsInstanceByCustomProperty()
    {
        var dbName = "saga_corrby_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            var orderNumber = "ORD-" + Guid.NewGuid().ToString("N")[..8];

            // Create the saga instance; the Then action sets OrderNumber from CausationId
            await bus.PublishAsync("saga.test.corr.create",
                new CreateOrder(),
                correlationId: correlationId,
                causationId: orderNumber);

            var state = await WaitForSagaStateAsync(db, correlationId, "Created");
            state.Should().NotBeNull();
            state!.OrderNumber.Should().Be(orderNumber);

            // Publish UpdateOrder with correlationId = orderNumber.
            // The CorrelateBy matches instance.OrderNumber == ctx.CorrelationId.
            await bus.PublishAsync("saga.test.corr.update",
                new UpdateOrder(),
                correlationId: orderNumber);

            // Wait for Updated state on the original instance
            var collection = db.GetCollection<CorrelateByState>("bus_saga_correlate-by-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            CorrelateByState? updated = null;
            while (DateTime.UtcNow < timeout)
            {
                updated = await collection
                    .Find(x => x.CorrelationId == correlationId)
                    .FirstOrDefaultAsync();

                if (updated?.CurrentState == "Updated")
                    break;

                await Task.Delay(100);
            }

            updated.Should().NotBeNull();
            updated!.CurrentState.Should().Be("Updated");
            updated.WasUpdated.Should().BeTrue();
            updated.OrderNumber.Should().Be(orderNumber);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
