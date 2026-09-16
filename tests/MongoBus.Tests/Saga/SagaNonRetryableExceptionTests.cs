using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaNonRetryableExceptionTests(MongoDbFixture fixture)
{
    private const string RejectOrderTypeId = "saga.test.non-retryable.reject-order";
    private const int MaxAttempts = 5;
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    public sealed class RejectOrder;

    /// <summary>
    /// A type of its own, so the test cannot pass because the saga machinery threw the same exception type for an
    /// unrelated reason of its own.
    /// </summary>
    public sealed class OrderRejectedException(string message) : Exception(message);

    public sealed class RejectionState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public class RejectionStateMachine : MongoBusStateMachine<RejectionState>
    {
        public SagaState Rejected { get; private set; }
        public SagaEvent<RejectOrder> OrderReceived { get; private set; }

        public RejectionStateMachine()
        {
            Event(() => OrderReceived, RejectOrderTypeId);

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderReceived)
                    .Then(_ => throw new OrderRejectedException("the order is not valid"))
                    .TransitionTo(Rejected));
        }
    }

    [Fact]
    public async Task ShouldDeadLetterWithoutRetryingWhenTheExceptionIsNotRetryable()
    {
        await using var bus = await StartBusAsync();
        var correlationId = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>()
            .PublishAsync(RejectOrderTypeId, new RejectOrder(), correlationId: correlationId);

        var outcome = await WaitForOutcomeAsync(bus, correlationId);

        outcome.Status.Should().Be("Dead",
            "the saga lists this exception as non-retryable, so its event must not be retried");
        outcome.Attempt.Should().Be(1,
            "a non-retryable exception should dead-letter on the first failure rather than after MaxAttempts");
    }

    private Task<RunningBus> StartBusAsync() =>
        RunningBus.StartAsync(fixture.ConnectionString, registerServices: services =>
            services.AddMongoBusSaga<RejectionStateMachine, RejectionState>(opt =>
            {
                opt.MaxAttempts = MaxAttempts;
                opt.RetryMode = ExceptionRetryMode.DenyList;
                opt.NoRetryExceptions = [typeof(OrderRejectedException)];
            }));

    /// <returns>
    /// The event once it has been dead-lettered, or once it has been retried, whichever happens first.
    /// </returns>
    private static async Task<InboxMessage> WaitForOutcomeAsync(RunningBus bus, string correlationId)
    {
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadline = DateTime.UtcNow.Add(WaitTimeout);

        while (DateTime.UtcNow < deadline)
        {
            var message = await inbox
                .Find(x => x.TypeId == RejectOrderTypeId && x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            if (message is not null && (message.Status == "Dead" || message.Attempt >= 2))
                return message;

            await Task.Delay(100);
        }

        throw new TimeoutException(
            $"The '{RejectOrderTypeId}' event was neither dead-lettered nor retried within {WaitTimeout}.");
    }
}
