using System.Collections.Concurrent;
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
public class SagaPartitionIsolationTests(MongoDbFixture fixture)
{
    private const string PickingStartedTypeId = "saga.test.partition-isolation.picking-started";
    private const string InvoiceRaisedTypeId = "saga.test.partition-isolation.invoice-raised";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    // --- Messages ---
    public sealed class PickingStarted;

    public sealed class InvoiceRaised;

    // --- Saga States ---
    public sealed class PickingState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public sealed class InvoicingState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    /// <summary>
    /// Keeps a picking saga's event in progress, and so its partition held, until the test releases it.
    /// </summary>
    public static class PickingInProgress
    {
        private static readonly ConcurrentDictionary<string, TaskCompletionSource> Releases = new();

        public static Task HoldAsync(string correlationId, CancellationToken ct) =>
            ReleaseFor(correlationId).Task.WaitAsync(ct);

        public static bool IsHeld(string correlationId) => Releases.ContainsKey(correlationId);

        public static void Release(string correlationId) => ReleaseFor(correlationId).TrySetResult();

        private static TaskCompletionSource ReleaseFor(string correlationId) =>
            Releases.GetOrAdd(correlationId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    // --- State Machines ---
    public class PickingStateMachine : MongoBusStateMachine<PickingState>
    {
        public SagaState Picking { get; private set; }

        public SagaEvent<PickingStarted> Started { get; private set; }

        public PickingStateMachine()
        {
            Event(() => Started, PickingStartedTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(Started)
                    .ThenAsync(ctx => PickingInProgress.HoldAsync(ctx.Saga.CorrelationId, ctx.CancellationToken))
                    .TransitionTo(Picking));
        }
    }

    public class InvoicingStateMachine : MongoBusStateMachine<InvoicingState>
    {
        public SagaState Invoiced { get; private set; }

        public SagaEvent<InvoiceRaised> Raised { get; private set; }

        public InvoicingStateMachine()
        {
            Event(() => Raised, InvoiceRaisedTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(Raised).TransitionTo(Invoiced));
        }
    }

    [Fact]
    public async Task Partitioned_Sagas_Should_Not_Wait_On_Each_Others_Partitions()
    {
        await using var bus = await StartBusAsync(invoicingPartitionCount: 1);

        await InvoicingShouldProgressWhilePickingHoldsItsPartitionAsync(bus);
    }

    [Fact]
    public async Task Saga_Without_Partitioning_Should_Not_Wait_On_Another_Sagas_Partition()
    {
        await using var bus = await StartBusAsync(invoicingPartitionCount: 0);

        await InvoicingShouldProgressWhilePickingHoldsItsPartitionAsync(bus);
    }

    private Task<RunningBus> StartBusAsync(int invoicingPartitionCount) =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
        {
            services.AddMongoBusSaga<PickingStateMachine, PickingState>(opt => opt.DefaultPartitionCount = 1);
            services.AddMongoBusSaga<InvoicingStateMachine, InvoicingState>(opt => opt.DefaultPartitionCount = invoicingPartitionCount);
        });

    private static async Task InvoicingShouldProgressWhilePickingHoldsItsPartitionAsync(RunningBus bus)
    {
        var picking = Guid.NewGuid().ToString("N");
        var invoicing = Guid.NewGuid().ToString("N");
        try
        {
            await PublishAsync(bus, PickingStartedTypeId, new PickingStarted(), picking);
            await WaitUntilAsync(() => Task.FromResult(PickingInProgress.IsHeld(picking)),
                $"picking saga {picking} to hold its partition");

            await PublishAsync(bus, InvoiceRaisedTypeId, new InvoiceRaised(), invoicing);

            await WaitUntilAsync(async () => await Sagas<InvoicingState>(bus).Find(x => x.CorrelationId == invoicing).AnyAsync(),
                $"invoicing saga {invoicing} to be created while an unrelated picking saga holds its own partition");
        }
        finally
        {
            PickingInProgress.Release(picking);
        }
    }

    private static Task PublishAsync<T>(RunningBus bus, string typeId, T message, string correlationId) =>
        bus.Services.GetRequiredService<IMessageBus>().PublishAsync(typeId, message, correlationId: correlationId);

    private static IMongoCollection<TInstance> Sagas<TInstance>(RunningBus bus) where TInstance : ISagaInstance =>
        bus.Database.GetCollection<TInstance>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(TInstance)));

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
