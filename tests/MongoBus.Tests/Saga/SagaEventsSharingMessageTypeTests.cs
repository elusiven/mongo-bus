using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaEventsSharingMessageTypeTests(MongoDbFixture fixture)
{
    private const string BookedTypeId = "saga.test.shared-message-type.booked";
    private const string DispatchedTypeId = "saga.test.shared-message-type.dispatched";
    private const string PickupDeadlineTypeId = "saga.test.shared-message-type.pickup-deadline";
    private const string DeliveryDeadlineTypeId = "saga.test.shared-message-type.delivery-deadline";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class ShipmentBooked;

    public sealed class ShipmentDispatched;

    public sealed class Deadline;

    // --- Saga State ---
    public sealed class ShipmentState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    // --- State Machine ---
    public class ShipmentStateMachine : MongoBusStateMachine<ShipmentState>
    {
        public SagaState AwaitingPickup { get; private set; }
        public SagaState InTransit { get; private set; }
        public SagaState PickupMissed { get; private set; }
        public SagaState DeliveryMissed { get; private set; }

        public SagaEvent<ShipmentBooked> Booked { get; private set; }
        public SagaEvent<ShipmentDispatched> Dispatched { get; private set; }
        public SagaEvent<Deadline> PickupDeadline { get; private set; }
        public SagaEvent<Deadline> DeliveryDeadline { get; private set; }

        public ShipmentStateMachine()
        {
            Event(() => Booked, BookedTypeId);
            Event(() => Dispatched, DispatchedTypeId);
            Event(() => PickupDeadline, PickupDeadlineTypeId);
            Event(() => DeliveryDeadline, DeliveryDeadlineTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(Booked).TransitionTo(AwaitingPickup),
                When(Dispatched).TransitionTo(InTransit));

            During(AwaitingPickup,
                When(PickupDeadline).TransitionTo(PickupMissed),
                When(DeliveryDeadline).TransitionTo(DeliveryMissed));

            During(InTransit,
                Ignore(PickupDeadline),
                When(DeliveryDeadline).TransitionTo(DeliveryMissed));
        }
    }

    [Fact]
    public async Task Events_Sharing_A_Message_Type_Should_Each_Run_Their_Own_Behaviour_In_The_Same_State()
    {
        await using var bus = await StartBusAsync();
        var correlationId = Guid.NewGuid().ToString("N");
        await PublishAsync(bus, BookedTypeId, new ShipmentBooked(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "AwaitingPickup");

        await PublishAsync(bus, PickupDeadlineTypeId, new Deadline(), correlationId);
        await WaitForHandledAsync(bus, PickupDeadlineTypeId, correlationId);

        (await FindSagaAsync(bus, correlationId)).CurrentState.Should().Be("PickupMissed",
            "the pickup deadline must run its own behaviour, not the delivery deadline's, although both carry a Deadline message");
    }

    [Fact]
    public async Task Ignoring_An_Event_Should_Not_Ignore_Another_Event_With_The_Same_Message_Type()
    {
        await using var bus = await StartBusAsync();
        var correlationId = Guid.NewGuid().ToString("N");
        await PublishAsync(bus, DispatchedTypeId, new ShipmentDispatched(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "InTransit");

        await PublishAsync(bus, DeliveryDeadlineTypeId, new Deadline(), correlationId);
        await WaitForHandledAsync(bus, DeliveryDeadlineTypeId, correlationId);

        (await FindSagaAsync(bus, correlationId)).CurrentState.Should().Be("DeliveryMissed",
            "only the pickup deadline is ignored in transit; the delivery deadline has its own behaviour there");
    }

    private Task<RunningBus> StartBusAsync() =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
            services.AddMongoBusSaga<ShipmentStateMachine, ShipmentState>(opt => opt.MaxAttempts = 1));

    private static Task PublishAsync<T>(RunningBus bus, string typeId, T message, string correlationId) =>
        bus.Services.GetRequiredService<IMessageBus>().PublishAsync(typeId, message, correlationId: correlationId);

    private static IMongoCollection<ShipmentState> Sagas(RunningBus bus) =>
        bus.Database.GetCollection<ShipmentState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(ShipmentState)));

    private static Task<ShipmentState> FindSagaAsync(RunningBus bus, string correlationId) =>
        Sagas(bus).Find(x => x.CorrelationId == correlationId).FirstAsync();

    private static Task WaitForSagaStateAsync(RunningBus bus, string correlationId, string state) =>
        WaitUntilAsync(async () =>
        {
            var saga = await Sagas(bus).Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            return saga?.CurrentState == state;
        }, $"saga {correlationId} to reach state '{state}'");

    private static Task WaitForHandledAsync(RunningBus bus, string typeId, string correlationId)
    {
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        return WaitUntilAsync(
            () => inbox.Find(x => x.TypeId == typeId && x.CorrelationId == correlationId && x.Status != "Pending").AnyAsync(),
            $"the '{typeId}' message for saga {correlationId} to be handled");
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTime.UtcNow.Add(WaitTimeout);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out after {WaitTimeout} waiting for {description}.");

            await Task.Delay(100);
        }
    }
}
