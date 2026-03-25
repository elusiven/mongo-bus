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
public class SagaExceptionHandlingTests(MongoDbFixture fixture)
{
    public sealed class ProcessOrder { public string Id { get; set; } = ""; public bool ShouldFail { get; set; } }

    public sealed class ExceptionTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ErrorMessage { get; set; }
        public string? ErrorType { get; set; }
        public int RetryCount { get; set; }
    }

    public class CatchSpecificExceptionStateMachine : MongoBusStateMachine<ExceptionTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Faulted { get; private set; }
        public SagaEvent<ProcessOrder> OrderProcessing { get; private set; }

        public CatchSpecificExceptionStateMachine()
        {
            Event(() => OrderProcessing, "saga.ex.catch.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderProcessing)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new InvalidOperationException("Business rule violated");
                    })
                    .TransitionTo(Processing)
                    .Catch<InvalidOperationException>(ex => ex
                        .Then(ctx =>
                        {
                            ctx.Saga.ErrorMessage = ctx.Exception.Message;
                            ctx.Saga.ErrorType = ctx.Exception.GetType().Name;
                        })
                        .TransitionTo(Faulted)));
        }
    }

    public class CatchAllStateMachine : MongoBusStateMachine<ExceptionTestState>
    {
        public SagaState Processing { get; private set; }
        public SagaState Error { get; private set; }
        public SagaEvent<ProcessOrder> OrderProcessing { get; private set; }

        public CatchAllStateMachine()
        {
            Event(() => OrderProcessing, "saga.ex.catchall.process", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderProcessing)
                    .Then(ctx =>
                    {
                        if (ctx.Message.ShouldFail)
                            throw new ArgumentException("Bad argument");
                    })
                    .TransitionTo(Processing)
                    .CatchAll(ex => ex
                        .Then(ctx => ctx.Saga.ErrorMessage = ctx.Exception.Message)
                        .TransitionTo(Error)));
        }
    }

    [Fact]
    public async Task Catch_SpecificException_TransitionsToFaultedState()
    {
        var dbName = "saga_ex_catch_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchSpecificExceptionStateMachine, ExceptionTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.ex.catch.process",
                new ProcessOrder { Id = "EX-1", ShouldFail = true },
                correlationId: cid);

            var collection = db.GetCollection<ExceptionTestState>("bus_saga_exception-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            ExceptionTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Faulted") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Faulted");
            state.ErrorMessage.Should().Be("Business rule violated");
            state.ErrorType.Should().Be("InvalidOperationException");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Catch_NoException_ExecutesNormally()
    {
        var dbName = "saga_ex_noex_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchSpecificExceptionStateMachine, ExceptionTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.ex.catch.process",
                new ProcessOrder { Id = "EX-2", ShouldFail = false },
                correlationId: cid);

            var collection = db.GetCollection<ExceptionTestState>("bus_saga_exception-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            ExceptionTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Processing");
            state.ErrorMessage.Should().BeNull();
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task CatchAll_CatchesAnyException()
    {
        var dbName = "saga_ex_catchall_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<CatchAllStateMachine, ExceptionTestState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.ex.catchall.process",
                new ProcessOrder { Id = "EX-3", ShouldFail = true },
                correlationId: cid);

            var collection = db.GetCollection<ExceptionTestState>("bus_saga_exception-test-state");
            var timeout = DateTime.UtcNow.AddSeconds(10);
            ExceptionTestState? state = null;
            while (DateTime.UtcNow < timeout)
            {
                state = await collection.Find(x => x.CorrelationId == cid).FirstOrDefaultAsync();
                if (state?.CurrentState == "Error") break;
                await Task.Delay(100);
            }

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Error");
            state.ErrorMessage.Should().Be("Bad argument");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }
}
