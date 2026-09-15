using System.Collections.Concurrent;
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
public class SagaInsertRaceTests(MongoDbFixture fixture)
{
    private const string OrderPlacedTypeId = "saga.test.insert-race.placed";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class OrderPlaced;

    // --- Saga State ---
    public sealed class InsertRaceState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    /// <summary>
    /// Creates the saga once, part-way through a first event's behaviour, as a worker handling another first event
    /// for the same saga would. The behaviour's own insert then collides with that saga.
    /// </summary>
    public static class CompetingSagaCreator
    {
        private static IMongoCollection<InsertRaceState>? _sagas;
        private static int _hasCreated;

        public static void Arm(IMongoCollection<InsertRaceState> sagas)
        {
            _sagas = sagas;
            Interlocked.Exchange(ref _hasCreated, 0);
        }

        public static async Task CreateOnceAsync(string correlationId)
        {
            if (Interlocked.Exchange(ref _hasCreated, 1) == 1)
                return;

            await _sagas!.InsertOneAsync(new InsertRaceState
            {
                CorrelationId = correlationId,
                CurrentState = "Placed",
                Version = 1,
                CreatedUtc = DateTime.UtcNow,
                LastModifiedUtc = DateTime.UtcNow
            });
        }
    }

    // --- State Machine ---
    public class InsertRaceStateMachine : MongoBusStateMachine<InsertRaceState>
    {
        public SagaState Placed { get; private set; }

        public SagaEvent<OrderPlaced> OrderPlacedEvent { get; private set; }

        public InsertRaceStateMachine()
        {
            Event(() => OrderPlacedEvent, OrderPlacedTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderPlacedEvent)
                    .ThenAsync(ctx => CompetingSagaCreator.CreateOnceAsync(ctx.Saga.CorrelationId))
                    .TransitionTo(Placed));

            During(Placed,
                Ignore(OrderPlacedEvent));
        }
    }

    [Fact]
    public async Task First_Event_Should_Fail_With_A_Concurrency_Conflict_When_Another_Event_Created_The_Saga_First()
    {
        var failures = new FailureRecorder();
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
        {
            services.AddMongoBusSaga<InsertRaceStateMachine, InsertRaceState>();
            services.AddSingleton<IConsumeObserver>(failures);
        });
        CompetingSagaCreator.Arm(bus.Database.GetCollection<InsertRaceState>(
            "bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(InsertRaceState))));
        var correlationId = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>()
            .PublishAsync(OrderPlacedTypeId, new OrderPlaced(), correlationId: correlationId);

        var failure = await failures.WaitForFirstAsync();
        failure.Should().BeOfType<SagaConcurrencyException>(
            "losing the race to create a saga is the same conflict as losing a version check, and must be reported as one");
    }

    private sealed class FailureRecorder : IConsumeObserver
    {
        private readonly ConcurrentQueue<Exception> _failures = new();

        public void OnMessageProcessed(ConsumeMetrics metrics)
        {
        }

        public void OnMessageFailed(ConsumeFailureMetrics metrics) => _failures.Enqueue(metrics.Exception);

        public async Task<Exception> WaitForFirstAsync()
        {
            var deadline = DateTime.UtcNow.Add(WaitTimeout);
            while (DateTime.UtcNow < deadline)
            {
                if (_failures.TryPeek(out var failure))
                    return failure;

                await Task.Delay(100);
            }

            throw new TimeoutException($"No message failed within {WaitTimeout}.");
        }
    }
}
