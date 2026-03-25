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
public class SagaFinalStateTests(MongoDbFixture fixture)
{
    public sealed class StartProcess { public string Id { get; set; } = ""; }
    public sealed class CompleteProcess { public string Id { get; set; } = ""; }

    public sealed class FinalTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public bool WasFinalized { get; set; }
    }

    // --- Auto-purge state machine ---
    public class AutoPurgeStateMachine : MongoBusStateMachine<FinalTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaEvent<StartProcess> Started { get; private set; }
        public SagaEvent<CompleteProcess> Completed { get; private set; }

        public AutoPurgeStateMachine()
        {
            Event(() => Started, "saga.final.auto.started", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => Completed, "saga.final.auto.completed", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(Started).TransitionTo(Processing));

            During(Processing,
                When(Completed)
                    .Then(ctx => ctx.Saga.WasFinalized = true)
                    .Finalize());

            SetCompletedWhenFinalized();
        }
    }

    // --- No-purge state machine ---
    public class NoPurgeStateMachine : MongoBusStateMachine<FinalTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaEvent<StartProcess> Started { get; private set; }
        public SagaEvent<CompleteProcess> Completed { get; private set; }

        public NoPurgeStateMachine()
        {
            Event(() => Started, "saga.final.nopurge.started", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => Completed, "saga.final.nopurge.completed", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(Started).TransitionTo(Processing));

            During(Processing,
                When(Completed)
                    .Then(ctx => ctx.Saga.WasFinalized = true)
                    .Finalize());

            // No SetCompletedWhenFinalized — instance should remain in DB
        }
    }

    [Fact]
    public async Task SetCompletedWhenFinalized_DeletesInstanceFromDb()
    {
        var dbName = "saga_final_purge_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<AutoPurgeStateMachine, FinalTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.final.auto.started",
                new StartProcess { Id = "P-1" }, correlationId: cid);

            // Wait for Processing state
            var collection = db.GetCollection<FinalTestState>("bus_saga_final-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            FinalTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }
            state.Should().NotBeNull();

            // Complete the process — should finalize and auto-purge
            await bus.PublishAsync("saga.final.auto.completed",
                new CompleteProcess { Id = "P-1" }, correlationId: cid);

            // Wait for deletion
            timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state == null) break;
                await Task.Delay(100);
            }

            state.Should().BeNull("instance should be purged after finalization");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task NoSetCompleted_InstanceRemainsInDb()
    {
        var dbName = "saga_final_nopurge_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<NoPurgeStateMachine, FinalTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.final.nopurge.started",
                new StartProcess { Id = "P-2" }, correlationId: cid);

            var collection = db.GetCollection<FinalTestState>("bus_saga_final-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            FinalTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            await bus.PublishAsync("saga.final.nopurge.completed",
                new CompleteProcess { Id = "P-2" }, correlationId: cid);

            // Wait for Final state
            timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Final") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull("instance should remain in DB without SetCompletedWhenFinalized");
            state!.CurrentState.Should().Be("Final");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Finalize_SetsCurrentStateToFinal()
    {
        var dbName = "saga_final_state_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<NoPurgeStateMachine, FinalTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.final.nopurge.started",
                new StartProcess { Id = "P-3" }, correlationId: cid);

            var collection = db.GetCollection<FinalTestState>("bus_saga_final-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout)
            {
                var s = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (s?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            await bus.PublishAsync("saga.final.nopurge.completed",
                new CompleteProcess { Id = "P-3" }, correlationId: cid);

            timeout = DateTime.UtcNow.AddSeconds(10);
            FinalTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Final") break;
                await Task.Delay(100);
            }

            state!.CurrentState.Should().Be("Final");
            state.WasFinalized.Should().BeTrue();
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }
}
