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
public class SagaMissingInstanceTests(MongoDbFixture fixture)
{
    public sealed class StartMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class FollowUpMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class MissingInstanceState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public bool FollowUpReceived { get; set; }
    }

    public class DiscardMissingStateMachine : MongoBusStateMachine<MissingInstanceState>
    {
        public SagaState Processing { get; private set; }

        public SagaEvent<StartMessage> StartEvent { get; private set; }
        public SagaEvent<FollowUpMessage> FollowUpEvent { get; private set; }

        public DiscardMissingStateMachine()
        {
            Event(() => StartEvent, "saga.test.missing.discard.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => FollowUpEvent, "saga.test.missing.discard.followup", e =>
            {
                e.CorrelateById(ctx => ctx.CorrelationId!);
                e.OnMissingInstance(cfg => cfg.Discard());
            });

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .TransitionTo(Processing));

            During(Processing,
                When(FollowUpEvent)
                    .Then(ctx => ctx.Saga.FollowUpReceived = true)
                    .TransitionTo(Processing));
        }
    }

    public class FaultMissingStateMachine : MongoBusStateMachine<MissingInstanceState>
    {
        public SagaState Processing { get; private set; }

        public SagaEvent<StartMessage> StartEvent { get; private set; }
        public SagaEvent<FollowUpMessage> FollowUpEvent { get; private set; }

        public FaultMissingStateMachine()
        {
            Event(() => StartEvent, "saga.test.missing.fault.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => FollowUpEvent, "saga.test.missing.fault.followup", e =>
            {
                e.CorrelateById(ctx => ctx.CorrelationId!);
                e.OnMissingInstance(cfg => cfg.Fault());
            });

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .TransitionTo(Processing));

            During(Processing,
                When(FollowUpEvent)
                    .Then(ctx => ctx.Saga.FollowUpReceived = true)
                    .TransitionTo(Processing));
        }
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

    [Fact]
    public async Task OnMissingInstance_Discard_SilentlyDropsEvent()
    {
        var dbName = "saga_missing_discard_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<DiscardMissingStateMachine, MissingInstanceState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var cid = Guid.NewGuid().ToString("N");

            // Publish follow-up without creating the saga first
            await bus.PublishAsync("saga.test.missing.discard.followup",
                new FollowUpMessage { Id = "MISS-1" },
                correlationId: cid);

            // Wait to ensure the message is processed (discarded)
            await Task.Delay(2000);

            var collection = db.GetCollection<MissingInstanceState>("bus_saga_missing-instance-state");
            var instance = await collection
                .Find(x => x.CorrelationId == cid)
                .FirstOrDefaultAsync();

            instance.Should().BeNull("event should be discarded when no saga instance exists");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task OnMissingInstance_Fault_ThrowsAndMessageGoesToRetry()
    {
        var dbName = "saga_missing_fault_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<FaultMissingStateMachine, MissingInstanceState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var cid = Guid.NewGuid().ToString("N");

            // Publish follow-up without creating the saga first
            await bus.PublishAsync("saga.test.missing.fault.followup",
                new FollowUpMessage { Id = "MISS-2" },
                correlationId: cid);

            // Wait for the message to be retried
            await Task.Delay(2000);

            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var message = await inbox
                .Find(x => x.TypeId == "saga.test.missing.fault.followup" && x.CorrelationId == cid)
                .FirstOrDefaultAsync();

            message.Should().NotBeNull();
            // The handler throws, so the message should have been retried (Attempt > 0) or moved to Dead status
            var hasRetried = message!.Attempt > 0 || message.Status == "Dead";
            hasRetried.Should().BeTrue("the fault behavior should cause the handler to throw, triggering retries");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
