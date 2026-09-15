using System.Collections.Concurrent;
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
public class SagaCatchCancellationTests(MongoDbFixture fixture)
{
    private const string StartWorkTypeId = "saga.test.catch-cancellation.start-work";
    private const string CallPartnerTypeId = "saga.test.catch-cancellation.call-partner";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class StartWork;

    public sealed class CallPartner;

    // --- Saga State ---
    public sealed class WorkState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public static class WorkRecorder
    {
        public static readonly ConcurrentDictionary<string, bool> Started = new();
        public static readonly ConcurrentDictionary<string, Exception> Caught = new();
    }

    // --- State Machine ---
    public class WorkStateMachine : MongoBusStateMachine<WorkState>
    {
        public SagaState Working { get; private set; }
        public SagaState Faulted { get; private set; }
        public SagaState PartnerTimedOut { get; private set; }

        public SagaEvent<StartWork> WorkStarted { get; private set; }
        public SagaEvent<CallPartner> PartnerCalled { get; private set; }

        public WorkStateMachine()
        {
            Event(() => WorkStarted, StartWorkTypeId);
            Event(() => PartnerCalled, CallPartnerTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(WorkStarted)
                    .ThenAsync(async ctx =>
                    {
                        WorkRecorder.Started[ctx.Saga.CorrelationId] = true;
                        await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
                    })
                    .TransitionTo(Working)
                    .CatchAll(ex => ex
                        .Then(ctx => WorkRecorder.Caught[ctx.Saga.CorrelationId] = ctx.Exception)
                        .TransitionTo(Faulted)),
                When(PartnerCalled)
                    .ThenAsync(async _ =>
                    {
                        using var partnerTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
                        await Task.Delay(Timeout.Infinite, partnerTimeout.Token);
                    })
                    .TransitionTo(Working)
                    .CatchAll(ex => ex.TransitionTo(PartnerTimedOut)));
        }
    }

    [Fact]
    public async Task CatchAll_Should_Not_Handle_The_Cancellation_Of_A_Stopping_Bus()
    {
        var bus = await StartBusAsync();
        var correlationId = Guid.NewGuid().ToString("N");
        try
        {
            await PublishAsync(bus, StartWorkTypeId, new StartWork(), correlationId);
            await WaitUntilAsync(() => Task.FromResult(WorkRecorder.Started.ContainsKey(correlationId)),
                $"saga {correlationId} to start working");
        }
        finally
        {
            await bus.DisposeAsync();
        }

        WorkRecorder.Caught.Should().NotContainKey(correlationId,
            "stopping the bus cancels the handler without failing the event, so there is nothing for the catch branch to handle");
        var startEvent = await bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName)
            .Find(x => x.TypeId == StartWorkTypeId && x.CorrelationId == correlationId).FirstAsync();
        startEvent.Status.Should().Be("Pending", "an event interrupted by shutdown is released for redelivery");
    }

    [Fact]
    public async Task CatchAll_Should_Handle_A_Cancellation_The_Bus_Did_Not_Request()
    {
        await using var bus = await StartBusAsync();
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishAsync(bus, CallPartnerTypeId, new CallPartner(), correlationId);

        await WaitUntilAsync(async () =>
        {
            var saga = await Sagas(bus).Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            return saga?.CurrentState == "PartnerTimedOut";
        }, $"saga {correlationId} to handle its partner call timing out");
    }

    private Task<RunningBus> StartBusAsync() =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
            services.AddMongoBusSaga<WorkStateMachine, WorkState>(opt => opt.MaxAttempts = 1));

    private static Task PublishAsync<T>(RunningBus bus, string typeId, T message, string correlationId) =>
        bus.Services.GetRequiredService<IMessageBus>().PublishAsync(typeId, message, correlationId: correlationId);

    private static IMongoCollection<WorkState> Sagas(RunningBus bus) =>
        bus.Database.GetCollection<WorkState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(WorkState)));

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
