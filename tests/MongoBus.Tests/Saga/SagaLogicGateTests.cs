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
public class SagaLogicGateTests(MongoDbFixture fixture)
{
    public sealed class SubmitOrder
    {
        public string OrderId { get; set; } = "";
        public decimal Amount { get; set; }
        public bool IsExpress { get; set; }
        public string PaymentType { get; set; } = "standard";
    }

    public sealed class LogicGateOrderState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public decimal Amount { get; set; }
        public bool WasExpress { get; set; }
        public string? PaymentType { get; set; }
        public string? RoutedTo { get; set; }
    }

    // --- If State Machine ---
    public class IfStateMachine : MongoBusStateMachine<LogicGateOrderState>
    {
        public SagaState ExpressProcessing { get; private set; }
        public SagaState StandardProcessing { get; private set; }
        public SagaEvent<SubmitOrder> OrderSubmittedEvent { get; private set; }

        public IfStateMachine()
        {
            Event(() => OrderSubmittedEvent, "saga.logic.if.submitted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderSubmittedEvent)
                    .Then(ctx => ctx.Saga.Amount = ctx.Message.Amount)
                    .If(ctx => ctx.Message.IsExpress,
                        then => then
                            .Then(ctx => ctx.Saga.WasExpress = true)
                            .TransitionTo(ExpressProcessing))
                    .If(ctx => !ctx.Message.IsExpress,
                        then => then
                            .TransitionTo(StandardProcessing)));
        }
    }

    // --- IfElse State Machine ---
    public class IfElseStateMachine : MongoBusStateMachine<LogicGateOrderState>
    {
        public SagaState HighValue { get; private set; }
        public SagaState LowValue { get; private set; }
        public SagaEvent<SubmitOrder> OrderSubmittedEvent { get; private set; }

        public IfElseStateMachine()
        {
            Event(() => OrderSubmittedEvent, "saga.logic.ifelse.submitted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderSubmittedEvent)
                    .Then(ctx => ctx.Saga.Amount = ctx.Message.Amount)
                    .IfElse(ctx => ctx.Message.Amount > 1000,
                        then => then.TransitionTo(HighValue),
                        @else => @else.TransitionTo(LowValue)));
        }
    }

    // --- Switch State Machine ---
    public class SwitchStateMachine : MongoBusStateMachine<LogicGateOrderState>
    {
        public SagaState CreditProcessing { get; private set; }
        public SagaState DebitProcessing { get; private set; }
        public SagaState CryptoProcessing { get; private set; }
        public SagaState UnknownPayment { get; private set; }
        public SagaEvent<SubmitOrder> OrderSubmittedEvent { get; private set; }

        public SwitchStateMachine()
        {
            Event(() => OrderSubmittedEvent, "saga.logic.switch.submitted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderSubmittedEvent)
                    .Then(ctx =>
                    {
                        ctx.Saga.PaymentType = ctx.Message.PaymentType;
                        ctx.Saga.Amount = ctx.Message.Amount;
                    })
                    .Switch(ctx => ctx.Message.PaymentType, cases => cases
                        .Case("credit", b => b
                            .Then(ctx => ctx.Saga.RoutedTo = "credit")
                            .TransitionTo(CreditProcessing))
                        .Case("debit", b => b
                            .Then(ctx => ctx.Saga.RoutedTo = "debit")
                            .TransitionTo(DebitProcessing))
                        .Case("crypto", b => b
                            .Then(ctx => ctx.Saga.RoutedTo = "crypto")
                            .TransitionTo(CryptoProcessing))
                        .Default(b => b
                            .Then(ctx => ctx.Saga.RoutedTo = "unknown")
                            .TransitionTo(UnknownPayment))));
        }
    }

    private static async Task<LogicGateOrderState?> WaitForSagaAsync(
        IMongoDatabase db, string collectionName, string correlationId, string expectedState, int timeoutSec = 10)
    {
        var collection = db.GetCollection<LogicGateOrderState>(collectionName);
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < timeout)
        {
            var inst = await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            if (inst?.CurrentState == expectedState) return inst;
            await Task.Delay(100);
        }
        return await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
    }

    // --- If Tests ---

    [Fact]
    public async Task If_ConditionTrue_ExecutesThenBranch()
    {
        var dbName = "saga_if_true_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.if.submitted",
                new SubmitOrder { OrderId = "IF-1", Amount = 500, IsExpress = true },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "ExpressProcessing");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("ExpressProcessing");
            state.WasExpress.Should().BeTrue();
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task If_ConditionFalse_SkipsThenBranch()
    {
        var dbName = "saga_if_false_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.if.submitted",
                new SubmitOrder { OrderId = "IF-2", Amount = 500, IsExpress = false },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "StandardProcessing");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("StandardProcessing");
            state.WasExpress.Should().BeFalse();
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    // --- IfElse Tests ---

    [Fact]
    public async Task IfElse_ConditionTrue_ExecutesThenBranch()
    {
        var dbName = "saga_ifelse_true_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfElseStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.ifelse.submitted",
                new SubmitOrder { OrderId = "IFE-1", Amount = 5000 },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "HighValue");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("HighValue");
            state.Amount.Should().Be(5000);
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task IfElse_ConditionFalse_ExecutesElseBranch()
    {
        var dbName = "saga_ifelse_false_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<IfElseStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.ifelse.submitted",
                new SubmitOrder { OrderId = "IFE-2", Amount = 50 },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "LowValue");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("LowValue");
            state.Amount.Should().Be(50);
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    // --- Switch Tests ---

    [Fact]
    public async Task Switch_MatchingCase_ExecutesCaseBranch()
    {
        var dbName = "saga_switch_credit_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<SwitchStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.switch.submitted",
                new SubmitOrder { OrderId = "SW-1", Amount = 100, PaymentType = "credit" },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "CreditProcessing");
            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("CreditProcessing");
            state.RoutedTo.Should().Be("credit");
            state.PaymentType.Should().Be("credit");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Switch_DifferentCase_ExecutesCorrectBranch()
    {
        var dbName = "saga_switch_crypto_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<SwitchStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.switch.submitted",
                new SubmitOrder { OrderId = "SW-2", Amount = 200, PaymentType = "crypto" },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "CryptoProcessing");
            state.Should().NotBeNull();
            state!.RoutedTo.Should().Be("crypto");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Switch_NoMatch_ExecutesDefaultBranch()
    {
        var dbName = "saga_switch_default_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o => { o.ConnectionString = fixture.ConnectionString; o.DatabaseName = dbName; });
        services.AddMongoBusSaga<SwitchStateMachine, LogicGateOrderState>();
        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);
            var cid = Guid.NewGuid().ToString("N");
            await bus.PublishAsync("saga.logic.switch.submitted",
                new SubmitOrder { OrderId = "SW-3", Amount = 300, PaymentType = "paypal" },
                correlationId: cid);

            var state = await WaitForSagaAsync(db, "bus_saga_logic-gate-order-state", cid, "UnknownPayment");
            state.Should().NotBeNull();
            state!.RoutedTo.Should().Be("unknown");
        }
        finally { foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None); }
    }
}
