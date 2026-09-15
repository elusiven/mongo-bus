using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class ConsumerStateWriteTests(MongoDbFixture fixture)
{
    private const string FailingTypeId = "state-writes.failing";
    private const string SucceedingTypeId = "state-writes.succeeding";
    private const string GatedTypeId = "state-writes.gated";
    private const string CancellableTypeId = "state-writes.cancellable";
    private const string FailingBatchTypeId = "state-writes.batch-failing";
    private const string GatedBatchTypeId = "state-writes.batch-gated";
    private const string CancellableBatchTypeId = "state-writes.batch-cancellable";
    private static readonly TimeSpan HandlerStartTimeout = TimeSpan.FromSeconds(10);

    public sealed record StateWriteMessage(string Text);

    // --- Single-message consumers ---

    public sealed class FailingHandler : IMessageHandler<StateWriteMessage>
    {
        public Task HandleAsync(StateWriteMessage message, ConsumeContext context, CancellationToken ct) =>
            throw new InvalidOperationException("handler failed");
    }

    public sealed class FailingDefinition : ConsumerDefinition<FailingHandler, StateWriteMessage>
    {
        public override string TypeId => FailingTypeId;
    }

    public sealed class SucceedingHandler : IMessageHandler<StateWriteMessage>
    {
        public Task HandleAsync(StateWriteMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class SucceedingDefinition : ConsumerDefinition<SucceedingHandler, StateWriteMessage>
    {
        public override string TypeId => SucceedingTypeId;
    }

    /// <summary>Waits for the test to release it and deliberately ignores cancellation.</summary>
    public sealed class GatedHandler : IMessageHandler<StateWriteMessage>
    {
        public static readonly Gate Gate = new();

        public async Task HandleAsync(StateWriteMessage message, ConsumeContext context, CancellationToken ct) =>
            await Gate.PassAsync();
    }

    public sealed class GatedDefinition : ConsumerDefinition<GatedHandler, StateWriteMessage>
    {
        public override string TypeId => GatedTypeId;
    }

    /// <summary>Runs until the bus stops and cancels it.</summary>
    public sealed class CancellableHandler : IMessageHandler<StateWriteMessage>
    {
        public static readonly Gate Gate = new();

        public async Task HandleAsync(StateWriteMessage message, ConsumeContext context, CancellationToken ct) =>
            await Gate.WaitForCancellationAsync(ct);
    }

    public sealed class CancellableDefinition : ConsumerDefinition<CancellableHandler, StateWriteMessage>
    {
        public override string TypeId => CancellableTypeId;
    }

    // --- Batch consumers ---

    public sealed class FailingBatchHandler : IBatchMessageHandler<StateWriteMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<StateWriteMessage> messages, BatchConsumeContext context, CancellationToken ct) =>
            throw new InvalidOperationException("batch handler failed");
    }

    public sealed class FailingBatchDefinition : BatchConsumerDefinition<FailingBatchHandler, StateWriteMessage>
    {
        public override string TypeId => FailingBatchTypeId;
        public override BatchConsumerOptions BatchOptions => SingleMessageBatches();
    }

    public sealed class GatedBatchHandler : IBatchMessageHandler<StateWriteMessage>
    {
        public static readonly Gate Gate = new();

        public async Task HandleBatchAsync(IReadOnlyList<StateWriteMessage> messages, BatchConsumeContext context, CancellationToken ct) =>
            await Gate.PassAsync();
    }

    public sealed class GatedBatchDefinition : BatchConsumerDefinition<GatedBatchHandler, StateWriteMessage>
    {
        public override string TypeId => GatedBatchTypeId;
        public override BatchConsumerOptions BatchOptions => SingleMessageBatches();
    }

    public sealed class CancellableBatchHandler : IBatchMessageHandler<StateWriteMessage>
    {
        public static readonly Gate Gate = new();

        public async Task HandleBatchAsync(IReadOnlyList<StateWriteMessage> messages, BatchConsumeContext context, CancellationToken ct) =>
            await Gate.WaitForCancellationAsync(ct);
    }

    public sealed class CancellableBatchDefinition : BatchConsumerDefinition<CancellableBatchHandler, StateWriteMessage>
    {
        public override string TypeId => CancellableBatchTypeId;
        public override BatchConsumerOptions BatchOptions => SingleMessageBatches();
    }

    // --- Lock taken over by another worker ---

    [Fact]
    public async Task Failing_Worker_Whose_Lock_Was_Taken_Over_Should_Not_Overwrite_The_Message_State()
    {
        var services = BuildServicesWithoutStarting(s => s.AddMongoBusConsumer<FailingHandler, StateWriteMessage, FailingDefinition>());
        var (staleCopy, takenOver) = await LockThenTakeOverAsync(services, new FailingDefinition().EndpointName, FailingTypeId);
        await MarkProcessedByNewOwnerAsync(services, takenOver);

        await services.GetRequiredService<IMessageDispatcher>()
            .DispatchAsync(staleCopy, ContextFor(staleCopy), CancellationToken.None);

        var stored = await FindAsync(services, staleCopy);
        stored.Status.Should().Be("Processed", "only the worker holding the lock may record the outcome");
        stored.Attempt.Should().Be(0);
    }

    [Fact]
    public async Task Succeeding_Worker_Whose_Lock_Was_Taken_Over_Should_Leave_The_New_Owner_In_Charge()
    {
        var services = BuildServicesWithoutStarting(s => s.AddMongoBusConsumer<SucceedingHandler, StateWriteMessage, SucceedingDefinition>());
        var (staleCopy, _) = await LockThenTakeOverAsync(services, new SucceedingDefinition().EndpointName, SucceedingTypeId);

        await services.GetRequiredService<IMessageDispatcher>()
            .DispatchAsync(staleCopy, ContextFor(staleCopy), CancellationToken.None);

        var stored = await FindAsync(services, staleCopy);
        stored.LockOwner.Should().Be("worker-b", "the worker that took the message over is still handling it");
        stored.Status.Should().Be("Pending");
    }

    [Fact]
    public async Task Failing_Batch_Worker_Whose_Lock_Was_Taken_Over_Should_Not_Overwrite_The_Message_State()
    {
        var services = BuildServicesWithoutStarting(s => s.AddMongoBusBatchConsumer<FailingBatchHandler, StateWriteMessage, FailingBatchDefinition>());
        var (staleCopy, takenOver) = await LockThenTakeOverAsync(services, new FailingBatchDefinition().EndpointName, FailingBatchTypeId);
        await MarkProcessedByNewOwnerAsync(services, takenOver);

        await services.GetRequiredService<IBatchMessageDispatcher>()
            .DispatchBatchAsync([staleCopy], BatchContextFor(staleCopy), CancellationToken.None);

        var stored = await FindAsync(services, staleCopy);
        stored.Status.Should().Be("Processed", "only the worker holding the lock may record the outcome");
        stored.Attempt.Should().Be(0);
    }

    // --- Bus stopping while a handler runs ---

    [Fact]
    public async Task Message_Whose_Handler_Completes_While_The_Bus_Stops_Should_Be_Marked_Processed()
    {
        await using var bus = await StartBusAsync(s => s.AddMongoBusConsumer<GatedHandler, StateWriteMessage, GatedDefinition>());

        var stored = await StopWhileHandlerCompletesAsync(bus, GatedTypeId, GatedHandler.Gate);

        stored.Status.Should().Be("Processed", "the handler finished, so stopping the bus must not lose its acknowledgement");
    }

    [Fact]
    public async Task Batch_Whose_Handler_Completes_While_The_Bus_Stops_Should_Be_Marked_Processed()
    {
        await using var bus = await StartBusAsync(s => s.AddMongoBusBatchConsumer<GatedBatchHandler, StateWriteMessage, GatedBatchDefinition>());

        var stored = await StopWhileHandlerCompletesAsync(bus, GatedBatchTypeId, GatedBatchHandler.Gate);

        stored.Status.Should().Be("Processed", "the batch was handled, so stopping the bus must not lose its acknowledgement");
    }

    [Fact]
    public async Task Message_Whose_Handler_Is_Cancelled_By_Stopping_The_Bus_Should_Be_Released_Without_Using_An_Attempt()
    {
        await using var bus = await StartBusAsync(s => s.AddMongoBusConsumer<CancellableHandler, StateWriteMessage, CancellableDefinition>());

        var stored = await StopWhileHandlerRunsAsync(bus, CancellableTypeId, CancellableHandler.Gate);

        ShouldBeReleasedWithoutUsingAnAttempt(stored);
    }

    [Fact]
    public async Task Batch_Whose_Handler_Is_Cancelled_By_Stopping_The_Bus_Should_Be_Released_Without_Using_An_Attempt()
    {
        await using var bus = await StartBusAsync(s => s.AddMongoBusBatchConsumer<CancellableBatchHandler, StateWriteMessage, CancellableBatchDefinition>());

        var stored = await StopWhileHandlerRunsAsync(bus, CancellableBatchTypeId, CancellableBatchHandler.Gate);

        ShouldBeReleasedWithoutUsingAnAttempt(stored);
    }

    private static void ShouldBeReleasedWithoutUsingAnAttempt(InboxMessage stored)
    {
        stored.Status.Should().Be("Pending");
        stored.Attempt.Should().Be(0, "being interrupted by shutdown is not a failed attempt");
        stored.LockOwner.Should().BeNull("another instance should be able to pick the message up straight away");
    }

    /// <summary>
    /// Stops the runtime and only then lets the handler finish. <see cref="BackgroundService.StopAsync"/>
    /// cancels the stopping token before it first awaits, so the handler completes after cancellation.
    /// </summary>
    private static async Task<InboxMessage> StopWhileHandlerCompletesAsync(RunningBus bus, string typeId, Gate gate)
    {
        await PublishAndWaitForHandlerAsync(bus, typeId, gate);

        var stopping = RuntimeOf(bus).StopAsync(CancellationToken.None);
        gate.Open();
        await stopping;

        return await FindByTypeAsync(bus.Services, typeId);
    }

    private static async Task<InboxMessage> StopWhileHandlerRunsAsync(RunningBus bus, string typeId, Gate gate)
    {
        await PublishAndWaitForHandlerAsync(bus, typeId, gate);

        await RuntimeOf(bus).StopAsync(CancellationToken.None);

        return await FindByTypeAsync(bus.Services, typeId);
    }

    private static async Task PublishAndWaitForHandlerAsync(RunningBus bus, string typeId, Gate gate)
    {
        gate.Reset();
        await bus.Services.GetRequiredService<IMessageBus>().PublishAsync(typeId, new StateWriteMessage("in flight"));
        await gate.Entered.WaitAsync(HandlerStartTimeout);
    }

    private Task<RunningBus> StartBusAsync(Action<IServiceCollection> registerConsumer) =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: registerConsumer);

    private ServiceProvider BuildServicesWithoutStarting(Action<IServiceCollection> registerConsumer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "state_writes_" + Guid.NewGuid().ToString("N");
        });
        registerConsumer(services);
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Worker A locks the message with a lock that expires at once; worker B then locks it for a minute.
    /// Returns A's now-stale copy and B's copy.
    /// </summary>
    private static async Task<(InboxMessage StaleCopy, InboxMessage TakenOver)> LockThenTakeOverAsync(
        IServiceProvider services,
        string endpointId,
        string typeId)
    {
        var pump = services.GetRequiredService<IMessagePump>();
        await services.GetRequiredService<ITopologyManager>().BindAsync(endpointId, typeId);
        await services.GetRequiredService<IMessageBus>().PublishAsync(typeId, new StateWriteMessage("taken over"));
        await Task.Delay(20);

        var staleCopy = (await pump.TryLockOneAsync(endpointId, TimeSpan.FromMilliseconds(1), "worker-a", CancellationToken.None))!;
        await Task.Delay(20);
        var takenOver = (await pump.TryLockOneAsync(endpointId, TimeSpan.FromMinutes(1), "worker-b", CancellationToken.None))!;

        return (staleCopy, takenOver);
    }

    private static Task MarkProcessedByNewOwnerAsync(IServiceProvider services, InboxMessage message) =>
        Inbox(services).UpdateOneAsync(
            x => x.Id == message.Id,
            Builders<InboxMessage>.Update
                .Set(x => x.Status, "Processed")
                .Set(x => x.ProcessedUtc, DateTime.UtcNow)
                .Set(x => x.LockOwner, null)
                .Set(x => x.LockedUntilUtc, null));

    private static BatchConsumerOptions SingleMessageBatches() => new()
    {
        MinBatchSize = 1,
        MaxBatchSize = 1,
        MaxBatchWaitTime = TimeSpan.FromMilliseconds(100),
        MaxBatchIdleTime = TimeSpan.Zero,
        FlushMode = BatchFlushMode.SinceFirstMessage
    };

    private static IMongoCollection<InboxMessage> Inbox(IServiceProvider services) =>
        services.GetRequiredService<IMongoDatabase>().GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static Task<InboxMessage> FindAsync(IServiceProvider services, InboxMessage message) =>
        Inbox(services).Find(x => x.Id == message.Id).SingleAsync();

    private static Task<InboxMessage> FindByTypeAsync(IServiceProvider services, string typeId) =>
        Inbox(services).Find(x => x.TypeId == typeId).SingleAsync();

    private static IHostedService RuntimeOf(RunningBus bus) =>
        bus.Services.GetServices<IHostedService>().OfType<MongoBusRuntime>().Single();

    private static ConsumeContext ContextFor(InboxMessage message) =>
        new(message.EndpointId, message.TypeId, message.Id, message.Attempt, null, "test", message.CloudEventId ?? "");

    private static BatchConsumeContext BatchContextFor(InboxMessage message) =>
        new(message.EndpointId, message.TypeId, [ContextFor(message)], DateTime.UtcNow, DateTime.UtcNow);

    /// <summary>Lets a test know a handler has started, and decide when it may finish.</summary>
    public sealed class Gate
    {
        private TaskCompletionSource _entered = NewSignal();
        private TaskCompletionSource _opened = NewSignal();

        public Task Entered => _entered.Task;

        public void Reset()
        {
            _entered = NewSignal();
            _opened = NewSignal();
        }

        public void Open() => _opened.TrySetResult();

        public Task PassAsync()
        {
            _entered.TrySetResult();
            return _opened.Task;
        }

        public Task WaitForCancellationAsync(CancellationToken ct)
        {
            _entered.TrySetResult();
            return Task.Delay(Timeout.Infinite, ct);
        }

        private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
