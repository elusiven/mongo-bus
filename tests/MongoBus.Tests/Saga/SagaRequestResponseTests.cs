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
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaRequestResponseTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class StartRequest
    {
        public string Item { get; set; } = "";
    }

    public sealed class DoWorkRequest
    {
        public string Item { get; set; } = "";
    }

    public sealed class WorkDoneResponse
    {
        public string Result { get; set; } = "";
    }

    public sealed class TriggerRespond
    {
        public string Value { get; set; } = "";
    }

    public sealed class RespondReply
    {
        public string Echo { get; set; } = "";
    }

    // --- Saga State (Request tests) ---
    public sealed class RequestTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? RequestId { get; set; }
        public bool ResponseReceived { get; set; }
        public bool TimedOut { get; set; }
    }

    // --- Saga State (Respond tests) ---
    public sealed class RespondTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? EchoValue { get; set; }
    }

    // --- Request State Machine ---
    public class RequestTestStateMachine : MongoBusStateMachine<RequestTestState>
    {
        public SagaState AwaitingWork { get; private set; }
        public SagaState Done { get; private set; }
        public SagaState TimedOutState { get; private set; }

        public SagaEvent<StartRequest> StartEvent { get; private set; }
        public SagaEvent<WorkDoneResponse> WorkDoneEvent { get; private set; }
        public SagaEvent<SagaTimeoutMessage> WorkTimeoutEvent { get; private set; }

        public SagaRequest<RequestTestState, DoWorkRequest, WorkDoneResponse> WorkRequest { get; }

        public RequestTestStateMachine()
        {
            WorkRequest = new SagaRequest<RequestTestState, DoWorkRequest, WorkDoneResponse>(
                "WorkRequest", "saga.test.req.do-work", "saga.test.req.work-done", TimeSpan.FromSeconds(3));
            WorkRequest.Pending = new SagaState("AwaitingWork");
            WorkRequest.Completed = new SagaEvent<WorkDoneResponse>("WorkCompleted", "saga.test.req.work-done");
            WorkRequest.Faulted = new SagaEvent<SagaFaultMessage>("WorkFaulted", "saga.test.req.do-work.fault");
            WorkRequest.TimeoutExpired = new SagaEvent<SagaTimeoutMessage>("WorkTimeout", "saga.test.req.do-work.timeout");

            Event(() => StartEvent, "saga.test.req.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => WorkDoneEvent, "saga.test.req.work-done", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => WorkTimeoutEvent, "saga.test.req.do-work.timeout", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .Request(WorkRequest,
                        ctx => new DoWorkRequest { Item = ctx.Message.Item },
                        (s, id) => s.RequestId = id));

            During(AwaitingWork,
                When(WorkDoneEvent)
                    .Then(ctx => ctx.Saga.ResponseReceived = true)
                    .TransitionTo(Done));

            During(AwaitingWork,
                When(WorkTimeoutEvent)
                    .Then(ctx => ctx.Saga.TimedOut = true)
                    .TransitionTo(TimedOutState));
        }
    }

    // --- Respond State Machine ---
    public class RespondTestStateMachine : MongoBusStateMachine<RespondTestState>
    {
        public SagaState Responded { get; private set; }

        public SagaEvent<TriggerRespond> TriggerEvent { get; private set; }

        public RespondTestStateMachine()
        {
            Event(() => TriggerEvent, "saga.test.respond.trigger", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(TriggerEvent)
                    .Then(ctx => ctx.Saga.EchoValue = ctx.Message.Value)
                    .Respond("saga.test.respond.reply",
                        ctx => new RespondReply { Echo = ctx.Message.Value })
                    .TransitionTo(Responded));
        }
    }

    // --- Respond handler to capture the reply ---
    public sealed class RespondReplyHandler : IMessageHandler<RespondReply>
    {
        public static readonly List<RespondReply> Received = [];
        public Task HandleAsync(RespondReply message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class RespondReplyDef : ConsumerDefinition<RespondReplyHandler, RespondReply>
    {
        public override string TypeId => "saga.test.respond.reply";
    }

    // --- Async Request State Machine ---
    public sealed class AsyncRequestTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? RequestId { get; set; }
        public bool ResponseReceived { get; set; }
    }

    public class AsyncRequestStateMachine : MongoBusStateMachine<AsyncRequestTestState>
    {
        public SagaState AwaitingWork { get; private set; }
        public SagaState Done { get; private set; }

        public SagaEvent<StartRequest> StartEvent { get; private set; }
        public SagaEvent<WorkDoneResponse> WorkDoneEvent { get; private set; }

        public SagaRequest<AsyncRequestTestState, DoWorkRequest, WorkDoneResponse> WorkRequest { get; }

        public AsyncRequestStateMachine()
        {
            WorkRequest = new SagaRequest<AsyncRequestTestState, DoWorkRequest, WorkDoneResponse>(
                "WorkRequest", "saga.test.asyncreq.do-work", "saga.test.asyncreq.work-done", TimeSpan.FromSeconds(3));
            WorkRequest.Pending = new SagaState("AwaitingWork");
            WorkRequest.Completed = new SagaEvent<WorkDoneResponse>("WorkCompleted", "saga.test.asyncreq.work-done");
            WorkRequest.Faulted = new SagaEvent<SagaFaultMessage>("WorkFaulted", "saga.test.asyncreq.do-work.fault");
            WorkRequest.TimeoutExpired = new SagaEvent<SagaTimeoutMessage>("WorkTimeout", "saga.test.asyncreq.do-work.timeout");

            Event(() => StartEvent, "saga.test.asyncreq.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            Event(() => WorkDoneEvent, "saga.test.asyncreq.work-done", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent)
                    .RequestAsync(WorkRequest,
                        async ctx =>
                        {
                            await Task.Delay(10);
                            return new DoWorkRequest { Item = ctx.Message.Item };
                        },
                        (s, id) => s.RequestId = id));

            During(AwaitingWork,
                When(WorkDoneEvent)
                    .Then(ctx => ctx.Saga.ResponseReceived = true)
                    .TransitionTo(Done));
        }
    }

    // --- Async Respond State Machine ---
    public sealed class AsyncRespondTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public class AsyncRespondStateMachine : MongoBusStateMachine<AsyncRespondTestState>
    {
        public SagaState Responded { get; private set; }

        public SagaEvent<TriggerRespond> TriggerEvent { get; private set; }

        public AsyncRespondStateMachine()
        {
            Event(() => TriggerEvent, "saga.test.asyncrespond.trigger", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(TriggerEvent)
                    .RespondAsync("saga.test.asyncrespond.reply",
                        async ctx =>
                        {
                            await Task.Delay(10);
                            return new RespondReply { Echo = ctx.Message.Value };
                        })
                    .TransitionTo(Responded));
        }
    }

    public sealed class AsyncRespondReplyHandler : IMessageHandler<RespondReply>
    {
        public static readonly List<RespondReply> Received = [];
        public Task HandleAsync(RespondReply message, ConsumeContext context, CancellationToken ct)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    public sealed class AsyncRespondReplyDef : ConsumerDefinition<AsyncRespondReplyHandler, RespondReply>
    {
        public override string TypeId => "saga.test.asyncrespond.reply";
    }

    // --- Helpers ---

    private (ServiceProvider sp, IMessageBus bus, IMongoDatabase db) BuildAndStart<TStateMachine, TInstance>(
        string dbName,
        Action<IServiceCollection>? additionalRegistrations = null)
        where TStateMachine : MongoBusStateMachine<TInstance>, new()
        where TInstance : class, ISagaInstance, new()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<TStateMachine, TInstance>();
        additionalRegistrations?.Invoke(services);

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

    private static async Task<T?> WaitForSagaStateAsync<T>(
        IMongoDatabase db,
        string collectionName,
        string correlationId,
        string expectedState,
        int timeoutSec = 10)
        where T : ISagaInstance
    {
        var collection = db.GetCollection<T>(collectionName);
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < timeout)
        {
            var instance = await collection
                .Find(Builders<T>.Filter.Eq(x => x.CorrelationId, correlationId))
                .FirstOrDefaultAsync();

            if (instance?.CurrentState == expectedState)
                return instance;

            await Task.Delay(100);
        }

        return await collection
            .Find(Builders<T>.Filter.Eq(x => x.CorrelationId, correlationId))
            .FirstOrDefaultAsync();
    }

    // --- Tests ---

    [Fact]
    public async Task Request_PublishesRequestAndTransitionsToPending()
    {
        var dbName = "saga_req_pending_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RequestTestStateMachine, RequestTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.req.start",
                new StartRequest { Item = "widget-1" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync<RequestTestState>(
                db, "bus_saga_request-test-state", correlationId, "AwaitingWork");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("AwaitingWork");
            state.RequestId.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Request_Completed_TransitionsOnResponse()
    {
        var dbName = "saga_req_done_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RequestTestStateMachine, RequestTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.req.start",
                new StartRequest { Item = "widget-2" },
                correlationId: correlationId);

            var pending = await WaitForSagaStateAsync<RequestTestState>(
                db, "bus_saga_request-test-state", correlationId, "AwaitingWork");
            pending.Should().NotBeNull();

            // Publish the response with the same correlationId
            await bus.PublishAsync("saga.test.req.work-done",
                new WorkDoneResponse { Result = "completed" },
                correlationId: correlationId);

            var done = await WaitForSagaStateAsync<RequestTestState>(
                db, "bus_saga_request-test-state", correlationId, "Done");

            done.Should().NotBeNull();
            done!.CurrentState.Should().Be("Done");
            done.ResponseReceived.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Request_Timeout_TransitionsOnExpiry()
    {
        var dbName = "saga_req_timeout_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RequestTestStateMachine, RequestTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 3);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.req.start",
                new StartRequest { Item = "widget-3" },
                correlationId: correlationId);

            var pending = await WaitForSagaStateAsync<RequestTestState>(
                db, "bus_saga_request-test-state", correlationId, "AwaitingWork");
            pending.Should().NotBeNull();

            // Do NOT send a response — wait for timeout (3s delay + processing time)
            var timedOut = await WaitForSagaStateAsync<RequestTestState>(
                db, "bus_saga_request-test-state", correlationId, "TimedOutState", timeoutSec: 15);

            timedOut.Should().NotBeNull();
            timedOut!.CurrentState.Should().Be("TimedOutState");
            timedOut.TimedOut.Should().BeTrue();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Respond_PublishesResponseMessage()
    {
        var dbName = "saga_respond_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RespondTestStateMachine, RespondTestState>(dbName,
            svc => svc.AddMongoBusConsumer<RespondReplyHandler, RespondReply, RespondReplyDef>());
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            RespondReplyHandler.Received.Clear();

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.respond.trigger",
                new TriggerRespond { Value = "hello" },
                correlationId: correlationId);

            // Wait for the reply handler to receive the response
            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && RespondReplyHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            RespondReplyHandler.Received.Should().ContainSingle();
            RespondReplyHandler.Received[0].Echo.Should().Be("hello");

            // Verify saga transitioned
            var state = await WaitForSagaStateAsync<RespondTestState>(
                db, "bus_saga_respond-test-state", correlationId, "Responded");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Responded");
            state.EchoValue.Should().Be("hello");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task RequestAsync_PublishesRequestAndTransitionsToPending()
    {
        var dbName = "saga_asyncreq_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<AsyncRequestStateMachine, AsyncRequestTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.asyncreq.start",
                new StartRequest { Item = "async-widget" },
                correlationId: correlationId);

            var state = await WaitForSagaStateAsync<AsyncRequestTestState>(
                db, "bus_saga_async-request-test-state", correlationId, "AwaitingWork");

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("AwaitingWork");
            state.RequestId.Should().NotBeNullOrEmpty();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task RespondAsync_PublishesResponseMessage()
    {
        var dbName = "saga_asyncrespond_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<AsyncRespondStateMachine, AsyncRespondTestState>(dbName,
            svc => svc.AddMongoBusConsumer<AsyncRespondReplyHandler, RespondReply, AsyncRespondReplyDef>());
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 2);
            AsyncRespondReplyHandler.Received.Clear();

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.asyncrespond.trigger",
                new TriggerRespond { Value = "async-hello" },
                correlationId: correlationId);

            var timeout = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < timeout && AsyncRespondReplyHandler.Received.Count == 0)
            {
                await Task.Delay(100);
            }

            AsyncRespondReplyHandler.Received.Should().ContainSingle();
            AsyncRespondReplyHandler.Received[0].Echo.Should().Be("async-hello");

            var state = await WaitForSagaStateAsync<AsyncRespondTestState>(
                db, "bus_saga_async-respond-test-state", correlationId, "Responded");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Responded");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
