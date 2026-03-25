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
public class SagaDuringAnyTests(MongoDbFixture fixture)
{
    public sealed class SubmitMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class AcceptMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class CancelMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class DuringAnyState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public bool WasCancelled { get; set; }
    }

    public class DuringAnyStateMachine : MongoBusStateMachine<DuringAnyState>
    {
        public SagaState Submitted { get; private set; }
        public SagaState Accepted { get; private set; }

        public SagaEvent<SubmitMessage> SubmitEvent { get; private set; }
        public SagaEvent<AcceptMessage> AcceptEvent { get; private set; }
        public SagaEvent<CancelMessage> CancelEvent { get; private set; }

        public DuringAnyStateMachine()
        {
            Event(() => SubmitEvent, "saga.test.any.submit", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => AcceptEvent, "saga.test.any.accept", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => CancelEvent, "saga.test.any.cancel", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(SubmitEvent)
                    .TransitionTo(Submitted));

            During(Submitted,
                When(AcceptEvent)
                    .TransitionTo(Accepted));

            DuringAny(
                When(CancelEvent)
                    .Then(ctx => ctx.Saga.WasCancelled = true)
                    .Finalize());
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
        services.AddMongoBusSaga<DuringAnyStateMachine, DuringAnyState>();

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

    private static async Task<DuringAnyState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<DuringAnyState>("bus_saga_during-any-state");
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
    public async Task DuringAny_HandlersApplyFromSubmittedState()
    {
        var dbName = "saga_any_submitted_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.any.submit",
                new SubmitMessage { Id = "ANY-1" },
                correlationId: cid);

            await WaitForSagaStateAsync(db, cid, "Submitted");

            await bus.PublishAsync("saga.test.any.cancel",
                new CancelMessage { Id = "ANY-1" },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Final");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Final");
            state.WasCancelled.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task DuringAny_HandlersApplyFromAcceptedState()
    {
        var dbName = "saga_any_accepted_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.any.submit",
                new SubmitMessage { Id = "ANY-2" },
                correlationId: cid);

            await WaitForSagaStateAsync(db, cid, "Submitted");

            await bus.PublishAsync("saga.test.any.accept",
                new AcceptMessage { Id = "ANY-2" },
                correlationId: cid);

            await WaitForSagaStateAsync(db, cid, "Accepted");

            await bus.PublishAsync("saga.test.any.cancel",
                new CancelMessage { Id = "ANY-2" },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Final");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Final");
            state.WasCancelled.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
