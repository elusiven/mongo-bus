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
public class SagaCatchRethrowTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class ProcessItem
    {
        public string ItemId { get; set; } = "";
        public bool ShouldFail { get; set; }
    }

    public sealed class AsyncProcessItem
    {
        public string ItemId { get; set; } = "";
        public bool ShouldFail { get; set; }
    }

    // --- Saga State ---
    public sealed class RethrowTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ErrorMessage { get; set; }
        public bool CatchExecuted { get; set; }
    }

    public sealed class AsyncCatchTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ErrorMessage { get; set; }
        public bool AsyncHandlerExecuted { get; set; }
    }

    // --- Rethrow State Machine ---
    public class RethrowStateMachine : MongoBusStateMachine<RethrowTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }

        public SagaEvent<ProcessItem> ProcessEvent { get; private set; }

        public RethrowStateMachine()
        {
            Event(() => ProcessEvent, "saga.test.rethrow.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new InvalidOperationException("Rethrow test failure");
                    })
                    .TransitionTo(Processing)
                    .Catch<InvalidOperationException>(ex => ex
                        .Then(ctx =>
                        {
                            ctx.Saga.CatchExecuted = true;
                            ctx.Saga.ErrorMessage = ctx.Exception.Message;
                        })
                        .Rethrow()));
        }
    }

    // --- CatchAll with ThenAsync State Machine ---
    public class AsyncCatchStateMachine : MongoBusStateMachine<AsyncCatchTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }

        public SagaEvent<AsyncProcessItem> ProcessEvent { get; private set; }

        public AsyncCatchStateMachine()
        {
            Event(() => ProcessEvent, "saga.test.asynccatch.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ProcessEvent)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new ArgumentException("Async catch test failure");
                    })
                    .TransitionTo(Processing)
                    .CatchAll(ex => ex
                        .ThenAsync(async ctx =>
                        {
                            await Task.Delay(10);
                            ctx.Saga.AsyncHandlerExecuted = true;
                            ctx.Saga.ErrorMessage = ctx.Exception.Message;
                        })
                        .TransitionTo(Faulted)));
        }
    }

    // --- Helpers ---

    private (ServiceProvider sp, IMessageBus bus, IMongoDatabase db) BuildAndStart<TStateMachine, TInstance>(
        string dbName)
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
        services.AddMongoBusSaga<TStateMachine, TInstance>(opt => opt.MaxAttempts = 1);

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

    // --- Tests ---

    [Fact]
    public async Task Catch_Rethrow_ExecutesCatchBranchThenRethrows()
    {
        var dbName = "saga_rethrow_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RethrowStateMachine, RethrowTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.rethrow.process",
                new ProcessItem { ItemId = "item-1", ShouldFail = true },
                correlationId: correlationId);

            // Because the exception is rethrown, the saga handler will fail.
            // The message will be retried and eventually dead-lettered.
            // The catch branch executes but the exception propagates, so the saga
            // state should NOT be persisted (the transaction rolls back on rethrow).
            // Wait for the inbox message to go to Dead status.
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var timeout = DateTime.UtcNow.AddSeconds(15);
            InboxMessage? deadMsg = null;
            while (DateTime.UtcNow < timeout)
            {
                deadMsg = await inbox
                    .Find(x => x.TypeId == "saga.test.rethrow.process"
                               && x.CorrelationId == correlationId
                               && x.Status == "Dead")
                    .FirstOrDefaultAsync();
                if (deadMsg != null) break;
                await Task.Delay(200);
            }

            deadMsg.Should().NotBeNull("the message should be dead-lettered after retries exhausted");
            deadMsg!.LastError.Should().Contain("Rethrow test failure");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Catch_Rethrow_SuccessPath_NoException()
    {
        var dbName = "saga_rethrow_ok_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<RethrowStateMachine, RethrowTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.rethrow.process",
                new ProcessItem { ItemId = "item-ok", ShouldFail = false },
                correlationId: correlationId);

            // No exception thrown, so normal transition to Processing
            var collection = db.GetCollection<RethrowTestState>("bus_saga_rethrow-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            RethrowTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection
                    .Find(x => x.CorrelationId == correlationId)
                    .FirstOrDefaultAsync();
                if (state?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Processing");
            state.CatchExecuted.Should().BeFalse();
            state.ErrorMessage.Should().BeNull();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task CatchAll_ThenAsync_ExecutesAsyncHandler()
    {
        var dbName = "saga_asynccatch_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<AsyncCatchStateMachine, AsyncCatchTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.asynccatch.process",
                new AsyncProcessItem { ItemId = "async-item", ShouldFail = true },
                correlationId: correlationId);

            var collection = db.GetCollection<AsyncCatchTestState>("bus_saga_async-catch-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            AsyncCatchTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection
                    .Find(x => x.CorrelationId == correlationId)
                    .FirstOrDefaultAsync();
                if (state?.CurrentState == "Faulted") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Faulted");
            state.AsyncHandlerExecuted.Should().BeTrue();
            state.ErrorMessage.Should().Be("Async catch test failure");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task CatchAll_ThenAsync_SuccessPath_NoException()
    {
        var dbName = "saga_asynccatch_ok_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart<AsyncCatchStateMachine, AsyncCatchTestState>(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, 1);

            var correlationId = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.test.asynccatch.process",
                new AsyncProcessItem { ItemId = "async-ok", ShouldFail = false },
                correlationId: correlationId);

            var collection = db.GetCollection<AsyncCatchTestState>("bus_saga_async-catch-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            AsyncCatchTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection
                    .Find(x => x.CorrelationId == correlationId)
                    .FirstOrDefaultAsync();
                if (state?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Processing");
            state.AsyncHandlerExecuted.Should().BeFalse();
            state.ErrorMessage.Should().BeNull();
        }
        finally
        {
            await StopAsync(hosted);
        }
    }
}
