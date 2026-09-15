using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class ClaimCheckCleanupTests(MongoDbFixture fixture)
{
    private const string TypeId = "cleanup.large.message";
    private const string EndpointId = "cleanup-endpoint";
    private static readonly TimeSpan CleanupInterval = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(10);

    public sealed record LargeMessage(string Value);

    public sealed class LargeMessageHandler : IMessageHandler<LargeMessage>
    {
        public Task HandleAsync(LargeMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class LargeMessageDefinition : ConsumerDefinition<LargeMessageHandler, LargeMessage>
    {
        public override string TypeId => "cleanup.large.message";
    }

    [Fact]
    public async Task CleanupService_ShouldDeleteOrphanedClaimChecks()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "cleanup_test_" + Guid.NewGuid().ToString("N");
            opt.ClaimCheck.Enabled = true;
            opt.ClaimCheck.ThresholdBytes = 10;
            opt.ClaimCheck.ProviderName = "memory";
            opt.ClaimCheck.Cleanup.Interval = TimeSpan.FromMilliseconds(500);
            opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.Zero; // Clean immediately for test
        });

        services.AddMongoBusConsumer<LargeMessageHandler, LargeMessage, LargeMessageDefinition>();
        services.AddMongoBusInMemoryClaimCheck();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();
        var provider = sp.GetRequiredService<IClaimCheckProvider>() as InMemoryClaimCheckProvider;

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // 1. Publish a message that uses claim-check
            await bus.PublishAsync("cleanup.large.message", new LargeMessage("Large enough payload"));

            // Wait for it to be stored
            await Task.Delay(500);

            // Verify it exists in memory provider
            var refsList = new List<ClaimCheckReference>();
            await foreach (var r in provider!.ListAsync(default)) refsList.Add(r);
            refsList.Should().HaveCount(1);
            var key = refsList[0].Key;

            // 2. Manually delete the InboxMessage to simulate TTL or manual deletion
            var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
            await inbox.DeleteManyAsync(_ => true);

            // 3. Wait for cleanup service to run
            await Task.Delay(2000);

            // 4. Verify claim-check is gone from provider
            refsList.Clear();
            await foreach (var r in provider!.ListAsync(default)) refsList.Add(r);
            refsList.Should().BeEmpty();
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Should_Keep_Payload_Referenced_By_A_Pending_Inbox_Message()
    {
        var storage = new InMemoryClaimCheckProvider();
        var orphan = await StorePayloadAsync(storage);

        await using var bus = await StartBusAsync(storage, beforeStart: async services =>
        {
            await services.GetRequiredService<ITopologyManager>().BindAsync(EndpointId, TypeId);
            await services.GetRequiredService<IMessageBus>().PublishAsync(TypeId, new LargeMessage("not consumed yet"));
        });
        await WaitForCleanupRunToFinishAsync(storage, orphan);

        (await StoredKeysAsync(storage)).Should().ContainSingle("the pending inbox message still needs its payload");
    }

    [Fact]
    public async Task Should_Keep_Payload_Referenced_By_A_Delayed_Outbox_Message()
    {
        var storage = new InMemoryClaimCheckProvider();
        var orphan = await StorePayloadAsync(storage);

        await using var bus = await StartBusAsync(
            storage,
            opt => opt.Outbox.Enabled = true,
            beforeStart: services => services.GetRequiredService<ITransactionalMessageBus>()
                .PublishToOutboxAsync(TypeId, new LargeMessage("delivered next month"), deliverAt: DateTime.UtcNow.AddDays(30)));
        await WaitForCleanupRunToFinishAsync(storage, orphan);

        (await StoredKeysAsync(storage)).Should().ContainSingle("the outbox relays the message only once it is due, so it still needs its payload");
    }

    [Fact]
    public async Task Should_Keep_Payload_Referenced_By_A_Message_Published_By_The_Python_Client()
    {
        var storage = new InMemoryClaimCheckProvider();
        var orphan = await StorePayloadAsync(storage);
        var referenced = await StorePayloadAsync(storage);

        await using var bus = await StartBusAsync(storage, beforeStart: services =>
            Inbox(services).InsertOneAsync(PendingMessage(PythonClientClaimCheckEnvelope(referenced))));
        await WaitForCleanupRunToFinishAsync(storage, orphan);

        (await StoredKeysAsync(storage)).Should().Equal(referenced.Key);
    }

    [Fact]
    public async Task Should_Delete_Orphaned_Payloads_When_Claim_Check_Is_Only_Requested_Per_Message()
    {
        var storage = new InMemoryClaimCheckProvider();
        var orphan = await StorePayloadAsync(storage);

        await using var bus = await StartBusAsync(storage, opt => opt.ClaimCheck.Enabled = false);

        await WaitUntilDeletedAsync(storage, orphan);
    }

    [Fact]
    public async Task Should_Delete_Orphaned_Payloads_When_A_Claim_Check_Message_Cannot_Be_Read()
    {
        var storage = new InMemoryClaimCheckProvider();
        var orphan = await StorePayloadAsync(storage);
        var truncatedEnvelope = $"{{\"dataContentType\":\"{ClaimCheckConstants.ContentType}\",\"data\":{{\"key\":";

        await using var bus = await StartBusAsync(storage, beforeStart: services =>
            Inbox(services).InsertOneAsync(PendingMessage(truncatedEnvelope)));

        await WaitUntilDeletedAsync(storage, orphan);
    }

    [Fact]
    public async Task Should_Read_The_Inbox_Once_Per_Run_However_Many_Payloads_It_Checks()
    {
        const int orphanCount = 20;
        var storage = new InMemoryClaimCheckProvider();
        for (var i = 0; i < orphanCount; i++)
            await StorePayloadAsync(storage);
        var inboxReads = new InboxReadCounter();

        await using var bus = await StartBusAsync(
            storage,
            opt => opt.ClaimCheck.Cleanup.Interval = TimeSpan.FromHours(1),
            registerServices: services => services.AddSingleton(inboxReads.CreateClient(fixture.ConnectionString)));
        await WaitUntilAsync(async () => (await StoredKeysAsync(storage)).Count == 0);

        (await StoredKeysAsync(storage)).Should().BeEmpty();
        inboxReads.Count.Should().Be(1);
    }

    private Task<RunningBus> StartBusAsync(
        InMemoryClaimCheckProvider storage,
        Action<MongoBusOptions>? configureOptions = null,
        Action<IServiceCollection>? registerServices = null,
        Func<IServiceProvider, Task>? beforeStart = null) =>
        RunningBus.StartAsync(
            fixture.ConnectionString,
            opt =>
            {
                opt.ClaimCheck.Enabled = true;
                opt.ClaimCheck.ProviderName = storage.Name;
                opt.ClaimCheck.ThresholdBytes = 1;
                opt.ClaimCheck.Cleanup.Interval = CleanupInterval;
                opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.Zero;
                configureOptions?.Invoke(opt);
            },
            services =>
            {
                services.AddSingleton<IClaimCheckProvider>(storage);
                registerServices?.Invoke(services);
            },
            beforeStart);

    private static Task<ClaimCheckReference> StorePayloadAsync(IClaimCheckProvider storage) =>
        storage.PutAsync(new ClaimCheckWriteRequest(new MemoryStream("{}"u8.ToArray())), CancellationToken.None);

    private static async Task<List<string>> StoredKeysAsync(IClaimCheckProvider storage)
    {
        var keys = new List<string>();
        await foreach (var reference in storage.ListAsync(CancellationToken.None))
            keys.Add(reference.Key);
        return keys;
    }

    private static async Task WaitUntilDeletedAsync(IClaimCheckProvider storage, ClaimCheckReference payload)
    {
        await WaitUntilAsync(async () => !(await StoredKeysAsync(storage)).Contains(payload.Key));

        (await StoredKeysAsync(storage)).Should().NotContain(payload.Key, "cleanup should delete payloads that no message references");
    }

    // Seeing one payload deleted does not mean the run has finished deleting the others.
    private static async Task WaitForCleanupRunToFinishAsync(IClaimCheckProvider storage, ClaimCheckReference orphan)
    {
        await WaitUntilDeletedAsync(storage, orphan);
        await Task.Delay(CleanupInterval);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.Add(CleanupTimeout);
        while (!await condition() && DateTime.UtcNow < deadline)
            await Task.Delay(100);
    }

    private static IMongoCollection<InboxMessage> Inbox(IServiceProvider services) =>
        services.GetRequiredService<IMongoDatabase>().GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static InboxMessage PendingMessage(string payloadJson) => new()
    {
        EndpointId = EndpointId,
        Topic = TypeId,
        TypeId = TypeId,
        PayloadJson = payloadJson,
        CreatedUtc = DateTime.UtcNow,
        VisibleUtc = DateTime.UtcNow,
        Status = "Pending",
        CloudEventId = Guid.NewGuid().ToString("N")
    };

    private static string PythonClientClaimCheckEnvelope(ClaimCheckReference reference) =>
        $$$"""{"specVersion": "1.0", "id": "{{{Guid.NewGuid():N}}}", "type": "{{{TypeId}}}", "source": "urn:python", "dataContentType": "{{{ClaimCheckConstants.ContentType}}}", "data": {"provider": "{{{reference.Provider}}}", "container": "{{{reference.Container}}}", "key": "{{{reference.Key}}}", "length": {{{reference.Length}}}}}""";

    private sealed class InboxReadCounter
    {
        private int _count;

        public int Count => _count;

        public IMongoClient CreateClient(string connectionString)
        {
            var settings = MongoClientSettings.FromConnectionString(connectionString);
            settings.ClusterConfigurator = cluster => cluster.Subscribe<CommandStartedEvent>(CountInboxReads);
            return new MongoClient(settings);
        }

        private void CountInboxReads(CommandStartedEvent command)
        {
            if (command.CommandName is "find" or "aggregate"
                && command.Command[command.CommandName] is BsonString { Value: MongoBusConstants.InboxCollectionName })
                Interlocked.Increment(ref _count);
        }
    }
}
