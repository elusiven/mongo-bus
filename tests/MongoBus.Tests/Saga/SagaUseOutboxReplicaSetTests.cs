using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using static MongoBus.Tests.Saga.SagaUseOutboxTests;

namespace MongoBus.Tests.Saga;

[Collection("Mongo replica set collection")]
public class SagaUseOutboxReplicaSetTests(MongoReplicaSetFixture fixture)
{
    private const string StartTypeId = "saga.useoutbox.start";
    private const string FollowUpTypeId = "saga.useoutbox.followup";
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task UseOutbox_Should_Commit_The_Saga_State_And_Relay_Its_Publishes_Through_The_Outbox()
    {
        await using var bus = await StartBusAsync(applicationName: null);
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishStartAsync(bus, correlationId, "hello");

        await WaitForSagaStateAsync(bus, correlationId, "Started");
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        await WaitUntilAsync(async () => await inbox.Find(x => x.TypeId == FollowUpTypeId).AnyAsync(),
            "the saga's follow-up to be relayed from the outbox to the inbox");
        (await bus.Database.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName)
            .Find(x => x.TypeId == FollowUpTypeId).AnyAsync()).Should()
            .BeTrue("with UseOutbox the saga's publish is staged in the outbox inside its transaction");
    }

    [Fact]
    public async Task UseOutbox_Should_Retry_The_Saga_Transaction_After_A_Transient_Commit_Error()
    {
        var applicationName = "saga-useoutbox-" + Guid.NewGuid().ToString("N");
        await using var bus = await StartBusAsync(applicationName);
        await using var commitError = await TransientCommitError.InjectOnceAsync(fixture.ConnectionString, applicationName);
        var correlationId = Guid.NewGuid().ToString("N");

        await PublishStartAsync(bus, correlationId, "retried");

        await WaitForSagaStateAsync(bus, correlationId, "Started");
        var startEvent = await WaitForProcessedStartEventAsync(bus, correlationId);
        startEvent.Attempt.Should().Be(0,
            "a commit error labelled TransientTransactionError is resolved by running the transaction again, not by failing the event");
    }

    private async Task<RunningBus> StartBusAsync(string? applicationName)
    {
        var bus = await RunningBus.StartAsync(
            new MongoUrlBuilder(fixture.ConnectionString) { ApplicationName = applicationName }.ToString(),
            opt =>
            {
                opt.Outbox.Enabled = true;
                opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(50);
            },
            services => services.AddMongoBusSaga<OutboxSagaStateMachine, OutboxSagaState>(opt => opt.UseOutbox = true));

        await bus.Services.GetRequiredService<ITopologyManager>().BindAsync("saga-useoutbox-followup", FollowUpTypeId);
        return bus;
    }

    private static Task PublishStartAsync(RunningBus bus, string correlationId, string payload) =>
        bus.Services.GetRequiredService<IMessageBus>()
            .PublishAsync(StartTypeId, new StartWorkflow { Payload = payload }, correlationId: correlationId);

    private static Task WaitForSagaStateAsync(RunningBus bus, string correlationId, string state)
    {
        var sagas = bus.Database.GetCollection<OutboxSagaState>("bus_saga_outbox-saga-state");
        return WaitUntilAsync(async () =>
            (await sagas.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync())?.CurrentState == state,
            $"saga {correlationId} to reach state '{state}'");
    }

    private static async Task<InboxMessage> WaitForProcessedStartEventAsync(RunningBus bus, string correlationId)
    {
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        InboxMessage? startEvent = null;
        await WaitUntilAsync(async () =>
        {
            startEvent = await inbox.Find(x => x.TypeId == StartTypeId && x.CorrelationId == correlationId).FirstOrDefaultAsync();
            return startEvent?.Status == "Processed";
        }, "the start event to be processed");
        return startEvent!;
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

    /// <summary>
    /// Makes MongoDB reject the next commitTransaction from one application with an error labelled
    /// TransientTransactionError, which clients are expected to resolve by running the transaction again.
    /// </summary>
    private sealed class TransientCommitError(IMongoDatabase admin) : IAsyncDisposable
    {
        private const int WriteConflict = 112;

        public static async Task<TransientCommitError> InjectOnceAsync(string connectionString, string applicationName)
        {
            var admin = new MongoClient(connectionString).GetDatabase("admin");
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = new BsonDocument("times", 1),
                ["data"] = new BsonDocument
                {
                    ["failCommands"] = new BsonArray { "commitTransaction" },
                    ["errorCode"] = WriteConflict,
                    ["errorLabels"] = new BsonArray { "TransientTransactionError" },
                    ["appName"] = applicationName
                }
            });
            return new TransientCommitError(admin);
        }

        public async ValueTask DisposeAsync() =>
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = "off"
            });
    }
}
