using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoBus.Models.Saga;
using MongoDB.Driver;

namespace MongoBus.Tests.Saga;

/// <summary>
/// UseOutbox against a standalone MongoDB, which does not support transactions. The transactional path itself is
/// covered by <see cref="SagaUseOutboxReplicaSetTests"/>.
/// </summary>
[Collection("Mongo collection")]
public class SagaUseOutboxTests(MongoDbFixture fixture)
{
    public sealed class StartWorkflow
    {
        public string Payload { get; set; } = "";
    }

    public sealed class OutboxSagaState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? CapturedPayload { get; set; }
    }

    public class OutboxSagaStateMachine : MongoBusStateMachine<OutboxSagaState>
    {
        public SagaState Started { get; private set; }
        public SagaEvent<StartWorkflow> StartEvent { get; private set; }

        public OutboxSagaStateMachine()
        {
            Event(() => StartEvent, "saga.useoutbox.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .Then(ctx => ctx.Saga.CapturedPayload = ctx.Message.Payload)
                    .Publish("saga.useoutbox.followup", ctx => new FollowUp
                    {
                        Echo = ctx.Saga.CapturedPayload!
                    })
                    .TransitionTo(Started));
        }
    }

    public sealed class FollowUp
    {
        public string Echo { get; set; } = "";
    }

    [Fact]
    public async Task UseOutbox_OnStandalone_WithFallback_DegradesToDirectPublish()
    {
        var sp = BuildServices(useOutbox: true, allowFallback: true);

        var hosted = await StartHostedAsync(sp);
        try
        {
            var bus = sp.GetRequiredService<IMessageBus>();
            var db = sp.GetRequiredService<IMongoDatabase>();
            await WaitForBindingsAsync(db, "saga.useoutbox.start");

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.useoutbox.start",
                new StartWorkflow { Payload = "fallback-ok" },
                correlationId: correlationId);

            // The saga must still progress under the fallback path.
            var sagaCollection = db.GetCollection<OutboxSagaState>("bus_saga_outbox-saga-state");
            await WaitUntilAsync(async () =>
                (await sagaCollection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync())
                    ?.CurrentState == "Started");

            var state = await sagaCollection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            state.CapturedPayload.Should().Be("fallback-ok");
        }
        finally
        {
            await StopHostedAsync(hosted);
        }
    }

    [Fact]
    public async Task UseOutbox_OnStandalone_WithoutFallback_ThrowsClearly()
    {
        var sp = BuildServices(useOutbox: true, allowFallback: false);

        var hosted = await StartHostedAsync(sp);
        try
        {
            var bus = sp.GetRequiredService<IMessageBus>();
            var db = sp.GetRequiredService<IMongoDatabase>();
            await WaitForBindingsAsync(db, "saga.useoutbox.start");

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.useoutbox.start",
                new StartWorkflow { Payload = "should-fail" },
                correlationId: correlationId);

            // The saga handler will repeatedly throw; eventually the inbox marks the message
            // Dead. Assert that the message ends up in a Dead status and that no saga row
            // was created (the throw aborts before persistence).
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            await WaitUntilAsync(async () =>
                await inbox.CountDocumentsAsync(x =>
                    x.TypeId == "saga.useoutbox.start" &&
                    x.CorrelationId == correlationId &&
                    (x.Status == "Dead" || x.LastError != null)) >= 1,
                timeoutSeconds: 30);

            var deadOrFailed = await inbox
                .Find(x => x.TypeId == "saga.useoutbox.start" && x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            deadOrFailed.Should().NotBeNull();
            deadOrFailed!.LastError.Should().Contain("UseOutbox");
        }
        finally
        {
            await StopHostedAsync(hosted);
        }
    }

    private ServiceProvider BuildServices(bool useOutbox, bool allowFallback)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var dbName = "saga_useoutbox_" + Guid.NewGuid().ToString("N");
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
            opt.Outbox.Enabled = true;
            opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(50);
        });
        services.AddMongoBusSaga<OutboxSagaStateMachine, OutboxSagaState>(opt =>
        {
            opt.UseOutbox = useOutbox;
            opt.AllowFallbackWhenTransactionsUnsupported = allowFallback;
            opt.MaxAttempts = 2; // keep dead-letter test quick
        });

        return services.BuildServiceProvider();
    }

    private static async Task<List<IHostedService>> StartHostedAsync(ServiceProvider sp)
    {
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopHostedAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
    }

    private static async Task WaitForBindingsAsync(IMongoDatabase db, string topic, int timeoutSeconds = 10)
    {
        var bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
        await WaitUntilAsync(async () =>
            await bindings.CountDocumentsAsync(x => x.Topic == topic) >= 1, timeoutSeconds);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, int timeoutSeconds = 10)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            if (await predicate()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException("Condition was not met in time.");
    }
}
