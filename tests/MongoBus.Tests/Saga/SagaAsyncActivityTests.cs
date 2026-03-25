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
public class SagaAsyncActivityTests(MongoDbFixture fixture)
{
    // --- Messages ---

    public sealed class ProcessMessage
    {
        public string Id { get; set; } = "";
    }

    public sealed class ConditionMessage
    {
        public string Id { get; set; } = "";
        public bool ShouldPass { get; set; }
    }

    public sealed class PublishTriggerMessage
    {
        public string Id { get; set; } = "";
        public string Content { get; set; } = "";
    }

    public sealed class AsyncNotification
    {
        public string Content { get; set; } = "";
    }

    // --- States ---

    public sealed class AsyncTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public bool AsyncProcessed { get; set; }
        public bool AsyncConditionMet { get; set; }
    }

    // --- Notification Consumer ---

    public sealed class AsyncNotificationHandler : IMessageHandler<AsyncNotification>
    {
        public static readonly List<AsyncNotification> Received = [];

        public Task HandleAsync(AsyncNotification message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class AsyncNotificationDef : ConsumerDefinition<AsyncNotificationHandler, AsyncNotification>
    {
        public override string TypeId => "saga.test.async.notification";
    }

    // --- State Machines ---

    public class AsyncActivityStateMachine : MongoBusStateMachine<AsyncTestState>
    {
        public SagaState Processed { get; private set; }
        public SagaEvent<ProcessMessage> ProcessEvent { get; private set; }

        public AsyncActivityStateMachine()
        {
            Event(() => ProcessEvent, "saga.test.async.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .ThenAsync(async ctx =>
                    {
                        await Task.Delay(10);
                        ctx.Saga.AsyncProcessed = true;
                    })
                    .TransitionTo(Processed));
        }
    }

    public class IfAsyncStateMachine : MongoBusStateMachine<AsyncTestState>
    {
        public SagaState Passed { get; private set; }
        public SagaState Skipped { get; private set; }
        public SagaEvent<ConditionMessage> ConditionEvent { get; private set; }

        public IfAsyncStateMachine()
        {
            Event(() => ConditionEvent, "saga.test.async.ifasync", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ConditionEvent)
                    .IfAsync(async ctx =>
                    {
                        await Task.Delay(10);
                        return ctx.Message.ShouldPass;
                    }, then => then
                        .Then(ctx => ctx.Saga.AsyncConditionMet = true)
                        .TransitionTo(Passed))
                    .IfAsync(async ctx =>
                    {
                        await Task.Delay(10);
                        return !ctx.Message.ShouldPass;
                    }, then => then
                        .TransitionTo(Skipped)));
        }
    }

    public class IfElseAsyncStateMachine : MongoBusStateMachine<AsyncTestState>
    {
        public SagaState TrueBranch { get; private set; }
        public SagaState FalseBranch { get; private set; }
        public SagaEvent<ConditionMessage> ConditionEvent { get; private set; }

        public IfElseAsyncStateMachine()
        {
            Event(() => ConditionEvent, "saga.test.async.ifelseasync", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ConditionEvent)
                    .IfElseAsync(async ctx =>
                    {
                        await Task.Delay(10);
                        return ctx.Message.ShouldPass;
                    },
                    then => then
                        .Then(ctx => ctx.Saga.AsyncConditionMet = true)
                        .TransitionTo(TrueBranch),
                    @else => @else
                        .TransitionTo(FalseBranch)));
        }
    }

    public class PublishAsyncStateMachine : MongoBusStateMachine<AsyncTestState>
    {
        public SagaState Published { get; private set; }
        public SagaEvent<PublishTriggerMessage> TriggerEvent { get; private set; }

        public PublishAsyncStateMachine()
        {
            Event(() => TriggerEvent, "saga.test.async.publishtrigger", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(TriggerEvent)
                    .PublishAsync("saga.test.async.notification", async ctx =>
                    {
                        await Task.Delay(10);
                        return new AsyncNotification { Content = ctx.Message.Content };
                    })
                    .TransitionTo(Published));
        }
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

    private static async Task<AsyncTestState?> WaitForSagaStateAsync(
        IMongoDatabase db,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
    {
        var collection = db.GetCollection<AsyncTestState>("bus_saga_async-test-state");
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
    public async Task ThenAsync_ExecutesAsyncAction()
    {
        var dbName = "saga_async_then_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<AsyncActivityStateMachine, AsyncTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.async.process",
                new ProcessMessage { Id = "ASYNC-1" },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Processed");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Processed");
            state.AsyncProcessed.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task IfAsync_EvaluatesAsyncCondition()
    {
        var dbName = "saga_async_ifasync_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfAsyncStateMachine, AsyncTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.async.ifasync",
                new ConditionMessage { Id = "ASYNC-2", ShouldPass = true },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "Passed");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Passed");
            state.AsyncConditionMet.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task IfElseAsync_EvaluatesAsyncCondition_TrueBranch()
    {
        var dbName = "saga_async_ifelse_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfElseAsyncStateMachine, AsyncTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.async.ifelseasync",
                new ConditionMessage { Id = "ASYNC-3", ShouldPass = true },
                correlationId: cid);

            var state = await WaitForSagaStateAsync(db, cid, "TrueBranch");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("TrueBranch");
            state.AsyncConditionMet.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task PublishAsync_PublishesMessageFromAsyncFactory()
    {
        var dbName = "saga_async_pub_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<PublishAsyncStateMachine, AsyncTestState>();
        services.AddMongoBusConsumer<AsyncNotificationHandler, AsyncNotification, AsyncNotificationDef>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            AsyncNotificationHandler.Received.Clear();

            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.async.publishtrigger",
                new PublishTriggerMessage { Id = "ASYNC-4", Content = "async-content" },
                correlationId: cid);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && AsyncNotificationHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            AsyncNotificationHandler.Received.Should().ContainSingle();
            AsyncNotificationHandler.Received[0].Content.Should().Be("async-content");

            var state = await WaitForSagaStateAsync(db, cid, "Published");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Published");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
