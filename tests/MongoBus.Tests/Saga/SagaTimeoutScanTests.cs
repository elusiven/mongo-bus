using FluentAssertions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaTimeoutScanTests(MongoDbFixture fixture)
{
    private const int SagasPerGroup = 20;
    private static readonly TimeSpan SagaTimeout = TimeSpan.FromHours(1);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class ShipmentBooked;

    // --- Saga State ---
    public sealed class ScanState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    // --- State Machine ---
    public class ScanStateMachine : MongoBusStateMachine<ScanState>
    {
        public SagaState Booked { get; private set; }

        public SagaEvent<ShipmentBooked> BookedEvent { get; private set; }

        public ScanStateMachine()
        {
            Event(() => BookedEvent, "saga.test.timeout-scan.booked");

            InstanceState(x => x.CurrentState);

            Initially(
                When(BookedEvent).TransitionTo(Booked));
        }
    }

    [Fact]
    public async Task Timeout_Scan_Should_Examine_Only_The_Sagas_It_Times_Out()
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
            services.AddMongoBusSaga<ScanStateMachine, ScanState>(opt =>
            {
                opt.SagaTimeout = SagaTimeout;
                opt.TimeoutScanInterval = TimeSpan.FromMilliseconds(200);
            }));
        await bus.Database.RunCommandAsync<BsonDocument>(new BsonDocument("profile", 2));
        var expiredSaga = NewSaga("Booked", age: SagaTimeout * 2);
        await Sagas(bus).InsertManyAsync(
        [
            expiredSaga,
            ..Enumerable.Range(0, SagasPerGroup).Select(_ => NewSaga("Booked", age: TimeSpan.Zero)),
            ..Enumerable.Range(0, SagasPerGroup).Select(_ => NewSaga("TimedOut", age: SagaTimeout * 2)),
            ..Enumerable.Range(0, SagasPerGroup).Select(_ => NewSaga("Final", age: SagaTimeout * 2))
        ]);

        await WaitForSagaStateAsync(bus, expiredSaga.CorrelationId, "TimedOut");

        var scan = await FindTheScanThatFoundOneSagaAsync(bus);
        scan["docsExamined"].ToInt64().Should().Be(1,
            $"the scan should read only the saga it times out, not active, timed-out or final sagas (plan: {scan["planSummary"]})");
    }

    private static ScanState NewSaga(string state, TimeSpan age) => new()
    {
        CorrelationId = Guid.NewGuid().ToString("N"),
        CurrentState = state,
        Version = 1,
        CreatedUtc = DateTime.UtcNow - age,
        LastModifiedUtc = DateTime.UtcNow - age
    };

    private static IMongoCollection<ScanState> Sagas(RunningBus bus) =>
        bus.Database.GetCollection<ScanState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(ScanState)));

    private static Task<BsonDocument> FindTheScanThatFoundOneSagaAsync(RunningBus bus)
    {
        var profile = bus.Database.GetCollection<BsonDocument>("system.profile");
        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("ns", Sagas(bus).CollectionNamespace.FullName),
            Builders<BsonDocument>.Filter.Eq("op", "query"),
            Builders<BsonDocument>.Filter.Eq("nreturned", 1),
            Builders<BsonDocument>.Filter.Exists("command.filter.CorrelationId", false));
        return profile.Find(filter).FirstAsync();
    }

    private static async Task WaitForSagaStateAsync(RunningBus bus, string correlationId, string state)
    {
        var deadline = DateTime.UtcNow.Add(WaitTimeout);
        while ((await Sagas(bus).Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync())?.CurrentState != state)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out after {WaitTimeout} waiting for saga {correlationId} to reach state '{state}'.");

            await Task.Delay(100);
        }
    }
}
