using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaCompositeEventRefireTests(MongoDbFixture fixture)
{
    private const string PaymentTypeId = "saga.test.composite-refire.payment";
    private const string StockTypeId = "saga.test.composite-refire.stock";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class PaymentReceived;

    public sealed class StockReserved;

    // --- Saga State ---
    public sealed class FulfilmentState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public int ReceivedFlags { get; set; }
        public int ReadyToShipCount { get; set; }
    }

    // --- State Machine ---
    public class FulfilmentStateMachine : MongoBusStateMachine<FulfilmentState>
    {
        public SagaState Collecting { get; private set; }

        public SagaEvent<PaymentReceived> Payment { get; private set; }
        public SagaEvent<StockReserved> Stock { get; private set; }
        public SagaEvent ReadyToShip { get; private set; }

        public FulfilmentStateMachine()
        {
            Event(() => Payment, PaymentTypeId);
            Event(() => Stock, StockTypeId);

            CompositeEvent(() => ReadyToShip, x => x.ReceivedFlags, Payment, Stock);

            When(ReadyToShip)
                .Then(s => s.ReadyToShipCount++)
                .Register();

            InstanceState(x => x.CurrentState);

            Initially(
                When(Payment).TransitionTo(Collecting),
                When(Stock).TransitionTo(Collecting));

            During(Collecting,
                When(Payment).Then(_ => { }),
                When(Stock).Then(_ => { }));
        }
    }

    [Fact]
    public async Task Composite_Event_Should_Fire_Once_When_A_Required_Event_Arrives_Again_After_It_Fired()
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString,
            registerServices: services => services.AddMongoBusSaga<FulfilmentStateMachine, FulfilmentState>());
        var messageBus = bus.Services.GetRequiredService<IMessageBus>();
        var correlationId = Guid.NewGuid().ToString("N");

        await messageBus.PublishAsync(PaymentTypeId, new PaymentReceived(), correlationId: correlationId);
        await WaitForVersionAsync(bus, correlationId, 1);
        await messageBus.PublishAsync(StockTypeId, new StockReserved(), correlationId: correlationId);
        await WaitForVersionAsync(bus, correlationId, 2);
        await messageBus.PublishAsync(PaymentTypeId, new PaymentReceived(), correlationId: correlationId);
        var saga = await WaitForVersionAsync(bus, correlationId, 3);

        saga.ReadyToShipCount.Should().Be(1,
            "the composite event was already satisfied, so a repeated payment event must not run its behaviour again");
    }

    private static async Task<FulfilmentState> WaitForVersionAsync(RunningBus bus, string correlationId, int version)
    {
        var sagas = bus.Database.GetCollection<FulfilmentState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(FulfilmentState)));
        var deadline = DateTime.UtcNow.Add(WaitTimeout);
        while (DateTime.UtcNow < deadline)
        {
            var saga = await sagas.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            if (saga?.Version == version)
                return saga;

            await Task.Delay(100);
        }

        throw new TimeoutException($"Saga {correlationId} did not reach version {version} within {WaitTimeout}.");
    }
}
