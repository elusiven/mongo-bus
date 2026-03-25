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
public class SagaIdempotencyTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class IncrementEvent
    {
        public string Label { get; private set; } = "";
    }

    // --- Saga State ---
    public sealed class IdempotentState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public int Counter { get; set; }
    }

    // --- State Machine ---
    public class IdempotentStateMachine : MongoBusStateMachine<IdempotentState>
    {
        public SagaState Counted { get; private set; }

        public SagaEvent<IncrementEvent> IncrementedEvent { get; private set; }

        public IdempotentStateMachine()
        {
            Event(() => IncrementedEvent, "saga.test.idemp.increment", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(IncrementedEvent)
                    .Then(ctx => ctx.Saga.Counter++)
                    .TransitionTo(Counted));

            During(Counted,
                When(IncrementedEvent)
                    .Then(ctx => ctx.Saga.Counter++)
                    .TransitionTo(Counted));
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
        services.AddMongoBusSaga<IdempotentStateMachine, IdempotentState>(
            configure: opt => opt.IdempotencyEnabled = true);

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

    private static async Task<IdempotentState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<IdempotentState>("bus_saga_idempotent-state");
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
    public async Task Idempotency_SameCloudEventIdProcessedOnce()
    {
        var dbName = "saga_idemp_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var correlationId = Guid.NewGuid().ToString("N");
            var cloudEventId = Guid.NewGuid().ToString("N");

            // Publish the same message twice with the same CloudEvent id
            await bus.PublishAsync("saga.test.idemp.increment",
                new IncrementEvent(),
                correlationId: correlationId,
                id: cloudEventId);

            var state = await WaitForSagaStateAsync(db, correlationId, "Counted");
            state.Should().NotBeNull();

            // Publish again with the same id
            await bus.PublishAsync("saga.test.idemp.increment",
                new IncrementEvent(),
                correlationId: correlationId,
                id: cloudEventId);

            // Wait to ensure the duplicate has time to be processed (and ignored)
            await Task.Delay(2000);

            var collection = db.GetCollection<IdempotentState>("bus_saga_idempotent-state");
            var latest = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            latest.Should().NotBeNull();
            latest!.Counter.Should().Be(1, "duplicate message with same CloudEvent id should be ignored");
            latest.Version.Should().Be(1);
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
