using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
public class SagaStaleScheduleTests(MongoDbFixture fixture)
{
    private const string OrderPlacedTypeId = "saga.test.stale-schedule.placed";
    private const string OrderPaidTypeId = "saga.test.stale-schedule.paid";
    private const string OrderCancelledTypeId = "saga.test.stale-schedule.cancelled";
    private const string ReminderTypeId = "saga.test.stale-schedule.reminder";
    private const string PaymentRequestTypeId = "saga.test.stale-schedule.payment-request";
    private const string PaymentTimeoutTypeId = PaymentRequestTypeId + ".timeout";

    // Long enough for the saga to complete or move on before the delayed message is delivered.
    private static readonly TimeSpan DeliveryDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    // --- Messages ---
    public sealed class OrderPlaced;

    public sealed class OrderPaid;

    public sealed class OrderCancelled;

    public sealed class PaymentReminder;

    public sealed class PaymentRequest;

    // --- Saga State ---
    public sealed class StaleScheduleState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public string? ReminderToken { get; set; }
        public string? PaymentRequestId { get; set; }
    }

    // --- State Machines ---
    public class ReminderStateMachine : MongoBusStateMachine<StaleScheduleState>
    {
        public SagaState AwaitingPayment { get; private set; }
        public SagaState Cancelled { get; private set; }

        public SagaEvent<OrderPlaced> Placed { get; private set; }
        public SagaEvent<OrderPaid> Paid { get; private set; }
        public SagaEvent<OrderCancelled> Cancel { get; private set; }
        public SagaEvent<PaymentReminder> ReminderDue { get; private set; }

        public SagaSchedule<StaleScheduleState, PaymentReminder> Reminder { get; } =
            new("Reminder", ReminderTypeId, DeliveryDelay);

        public ReminderStateMachine()
        {
            Event(() => Placed, OrderPlacedTypeId);
            Event(() => Paid, OrderPaidTypeId);
            Event(() => Cancel, OrderCancelledTypeId);
            Event(() => ReminderDue, ReminderTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(Placed)
                    .Schedule(Reminder, _ => new PaymentReminder(), (s, token) => s.ReminderToken = token)
                    .TransitionTo(AwaitingPayment));

            During(AwaitingPayment,
                When(Paid)
                    .Unschedule<PaymentReminder>((s, token) => s.ReminderToken = token)
                    .Finalize(),
                When(Cancel)
                    .Unschedule<PaymentReminder>((s, token) => s.ReminderToken = token)
                    .TransitionTo(Cancelled),
                When(ReminderDue)
                    .Then(_ => { }));

            SetCompletedWhenFinalized();
        }
    }

    public class PaymentRequestStateMachine : MongoBusStateMachine<StaleScheduleState>
    {
        public SagaEvent<OrderPlaced> Placed { get; private set; }
        public SagaEvent<OrderPaid> Paid { get; private set; }
        public SagaEvent<SagaTimeoutMessage> PaymentTimedOut { get; private set; }

        public SagaRequest<StaleScheduleState, PaymentRequest, OrderPaid> Payment { get; } =
            new("Payment", PaymentRequestTypeId, OrderPaidTypeId, DeliveryDelay) { Pending = new SagaState("AwaitingPayment") };

        public PaymentRequestStateMachine()
        {
            Event(() => Placed, OrderPlacedTypeId);
            Event(() => Paid, OrderPaidTypeId);
            Event(() => PaymentTimedOut, PaymentTimeoutTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(Placed)
                    .Request(Payment, _ => new PaymentRequest(), (s, requestId) => s.PaymentRequestId = requestId));

            During(Payment.Pending,
                When(Paid).Finalize(),
                When(PaymentTimedOut).Then(_ => { }));

            SetCompletedWhenFinalized();
        }
    }

    [Fact]
    public async Task Scheduled_Message_Should_Be_Discarded_When_Its_Saga_Completed_Before_Delivery()
    {
        await using var bus = await StartBusAsync<ReminderStateMachine>();
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishAsync(bus, OrderPlacedTypeId, new OrderPlaced(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "AwaitingPayment");
        await PublishAsync(bus, OrderPaidTypeId, new OrderPaid(), correlationId);
        await WaitForSagaDeletedAsync(bus, correlationId);

        var reminder = await WaitForHandledAsync(bus, ReminderTypeId, correlationId);
        reminder.Status.Should().Be("Processed",
            "the saga completed and cannot cancel the reminder it scheduled, so the reminder is stale rather than a failure");
    }

    [Fact]
    public async Task Request_Timeout_Should_Be_Discarded_When_Its_Saga_Completed_Before_Delivery()
    {
        await using var bus = await StartBusAsync<PaymentRequestStateMachine>();
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishAsync(bus, OrderPlacedTypeId, new OrderPlaced(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "AwaitingPayment");
        await PublishAsync(bus, OrderPaidTypeId, new OrderPaid(), correlationId);
        await WaitForSagaDeletedAsync(bus, correlationId);

        var timeout = await WaitForHandledAsync(bus, PaymentTimeoutTypeId, correlationId);
        timeout.Status.Should().Be("Processed",
            "the request was answered and the saga completed, so its timeout is stale rather than a failure");
    }

    [Fact]
    public async Task Scheduled_Message_Should_Be_Discarded_Quietly_When_Its_Saga_Moved_On_Before_Delivery()
    {
        var warnings = new WarningCollector();
        await using var bus = await StartBusAsync<ReminderStateMachine>(warnings);
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishAsync(bus, OrderPlacedTypeId, new OrderPlaced(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "AwaitingPayment");
        await PublishAsync(bus, OrderCancelledTypeId, new OrderCancelled(), correlationId);
        await WaitForSagaStateAsync(bus, correlationId, "Cancelled");

        var reminder = await WaitForHandledAsync(bus, ReminderTypeId, correlationId);
        reminder.Status.Should().Be("Processed");
        warnings.FromSagaEventHandlers.Should().BeEmpty(
            "a reminder that arrives after its saga was cancelled is expected, not a sign of a misconfigured state machine");
    }

    private Task<RunningBus> StartBusAsync<TStateMachine>(WarningCollector? warnings = null)
        where TStateMachine : MongoBusStateMachine<StaleScheduleState>, new() =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
        {
            services.AddMongoBusSaga<TStateMachine, StaleScheduleState>(opt => opt.MaxAttempts = 1);
            if (warnings != null)
                services.AddSingleton<ILoggerProvider>(warnings);
        });

    private static Task PublishAsync<T>(RunningBus bus, string typeId, T message, string correlationId) =>
        bus.Services.GetRequiredService<IMessageBus>().PublishAsync(typeId, message, correlationId: correlationId);

    private static IMongoCollection<StaleScheduleState> Sagas(RunningBus bus) =>
        bus.Database.GetCollection<StaleScheduleState>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(StaleScheduleState)));

    private static Task WaitForSagaStateAsync(RunningBus bus, string correlationId, string state) =>
        WaitUntilAsync(async () =>
        {
            var saga = await Sagas(bus).Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
            return saga?.CurrentState == state;
        }, $"saga {correlationId} to reach state '{state}'");

    private static Task WaitForSagaDeletedAsync(RunningBus bus, string correlationId) =>
        WaitUntilAsync(async () => await Sagas(bus).Find(x => x.CorrelationId == correlationId).AnyAsync() == false,
            $"saga {correlationId} to be deleted");

    private static async Task<InboxMessage> WaitForHandledAsync(RunningBus bus, string typeId, string correlationId)
    {
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        InboxMessage? message = null;
        await WaitUntilAsync(async () =>
        {
            message = await inbox.Find(x => x.TypeId == typeId && x.CorrelationId == correlationId).FirstOrDefaultAsync();
            return message is { Status: not "Pending" };
        }, $"the '{typeId}' message for saga {correlationId} to be handled");
        return message!;
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

    private sealed class WarningCollector : ILoggerProvider
    {
        private readonly ConcurrentQueue<string> _sagaEventHandlerWarnings = new();

        public IReadOnlyCollection<string> FromSagaEventHandlers => _sagaEventHandlerWarnings;

        public ILogger CreateLogger(string categoryName) =>
            categoryName.Contains("SagaEventHandler")
                ? new WarningLogger(_sagaEventHandlerWarnings)
                : Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        public void Dispose()
        {
        }

        private sealed class WarningLogger(ConcurrentQueue<string> warnings) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (IsEnabled(logLevel))
                    warnings.Enqueue(formatter(state, exception));
            }
        }
    }
}
