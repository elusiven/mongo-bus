using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaPublishAfterStateWriteTests(MongoDbFixture fixture)
{
    private const string StartTypeId = "saga.test.publish-after-write.start";
    private const string DispatchTypeId = "saga.test.publish-after-write.dispatch";
    private const string NoticeTypeId = "saga.test.publish-after-write.notice";
    private static readonly TimeSpan StateTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class StartShipment;

    public sealed class DispatchShipment;

    public sealed class ShipmentDispatched
    {
        public string CorrelationId { get; set; } = "";
    }

    // --- Saga State ---
    public sealed class ShipmentState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    /// <summary>
    /// Changes the stored saga once, part-way through a behaviour, as a worker handling another event for the
    /// same saga would. The behaviour's own state write then fails its version check.
    /// </summary>
    public static class ConcurrentSagaWriter
    {
        private static IMongoCollection<ShipmentState>? _sagas;
        private static int _hasWritten;

        public static void Arm(IMongoCollection<ShipmentState> sagas)
        {
            _sagas = sagas;
            Interlocked.Exchange(ref _hasWritten, 0);
        }

        public static async Task WriteOnceAsync(string correlationId)
        {
            if (Interlocked.Exchange(ref _hasWritten, 1) == 1)
                return;

            await _sagas!.UpdateOneAsync(
                x => x.CorrelationId == correlationId,
                Builders<ShipmentState>.Update.Inc(x => x.Version, 1));
        }
    }

    // --- State Machine ---
    public class ShipmentStateMachine : MongoBusStateMachine<ShipmentState>
    {
        public SagaState Started { get; private set; }
        public SagaState Dispatched { get; private set; }

        public SagaEvent<StartShipment> ShipmentStarted { get; private set; }
        public SagaEvent<DispatchShipment> DispatchRequested { get; private set; }

        public ShipmentStateMachine()
        {
            Event(() => ShipmentStarted, StartTypeId, e => e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => DispatchRequested, DispatchTypeId, e => e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ShipmentStarted)
                    .TransitionTo(Started));

            During(Started,
                When(DispatchRequested)
                    .ThenAsync(ctx => ConcurrentSagaWriter.WriteOnceAsync(ctx.Saga.CorrelationId))
                    .Publish(NoticeTypeId, ctx => new ShipmentDispatched { CorrelationId = ctx.Saga.CorrelationId })
                    .TransitionTo(Dispatched));
        }
    }

    // --- Consumer of the published notice, so it is routed to an inbox ---
    public sealed class NoticeHandler : IMessageHandler<ShipmentDispatched>
    {
        public Task HandleAsync(ShipmentDispatched message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class NoticeDefinition : ConsumerDefinition<NoticeHandler, ShipmentDispatched>
    {
        public override string TypeId => NoticeTypeId;
    }

    [Fact]
    public async Task Saga_Should_Publish_Only_For_The_Transition_It_Persisted()
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
        {
            services.AddMongoBusSaga<ShipmentStateMachine, ShipmentState>();
            services.AddMongoBusConsumer<NoticeHandler, ShipmentDispatched, NoticeDefinition>();
        });
        var sagas = bus.Database.GetCollection<ShipmentState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(ShipmentState)));
        var messageBus = bus.Services.GetRequiredService<IMessageBus>();
        var correlationId = Guid.NewGuid().ToString("N");
        ConcurrentSagaWriter.Arm(sagas);

        await messageBus.PublishAsync(StartTypeId, new StartShipment(), correlationId: correlationId);
        await WaitForStateAsync(sagas, correlationId, "Started");
        await messageBus.PublishAsync(DispatchTypeId, new DispatchShipment(), correlationId: correlationId);
        await WaitForStateAsync(sagas, correlationId, "Dispatched");

        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        await WaitForProcessedAsync(inbox, DispatchTypeId, correlationId);
        (await inbox.CountDocumentsAsync(x => x.TypeId == NoticeTypeId)).Should().Be(1,
            "the first attempt's state write lost to a concurrent change, so only the retry that persisted the transition may publish");
    }

    // The saga's publishes are sent after its state write, so reaching the new state does not mean the notice was sent;
    // the dispatch event is marked processed only once its handler, including that publish, has returned.
    private static async Task WaitForProcessedAsync(IMongoCollection<InboxMessage> inbox, string typeId, string correlationId)
    {
        var deadline = DateTime.UtcNow.Add(StateTimeout);
        while (!await inbox.Find(x => x.TypeId == typeId && x.CorrelationId == correlationId && x.Status == "Processed").AnyAsync())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"The '{typeId}' message for saga {correlationId} was not processed within {StateTimeout}.");

            await Task.Delay(100);
        }
    }

    private static async Task WaitForStateAsync(IMongoCollection<ShipmentState> sagas, string correlationId, string state)
    {
        var deadline = DateTime.UtcNow.Add(StateTimeout);
        while (DateTime.UtcNow < deadline)
        {
            var instance = await sagas.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            if (instance?.CurrentState == state)
                return;

            await Task.Delay(100);
        }

        throw new TimeoutException($"Saga {correlationId} did not reach state '{state}' within {StateTimeout}.");
    }
}
