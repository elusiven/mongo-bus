using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaTtlTests(MongoDbFixture fixture)
{
    // --- Messages ---
    public sealed class StartEvent
    {
        public string Id { get; private set; } = "";
    }

    // --- Saga State ---
    public sealed class TtlTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    // --- State Machine ---
    public class TtlStateMachine : MongoBusStateMachine<TtlTestState>
    {
        public SagaState Started { get; private set; }

        public SagaEvent<StartEvent> StartedEvent { get; private set; }

        public TtlStateMachine()
        {
            Event(() => StartedEvent, "saga.test.ttl.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartedEvent)
                    .TransitionTo(Started));
        }
    }

    private (ServiceProvider sp, IMessageBus bus, IMongoDatabase db) BuildAndStart(string dbName)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<TtlStateMachine, TtlTestState>(
            configure: opt => opt.SagaInstanceTtl = TimeSpan.FromDays(1));

        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredService<IMessageBus>(), sp.GetRequiredService<IMongoDatabase>());
    }

    private static async Task<List<IHostedService>> StartAsync(ServiceProvider sp)
    {
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopAsync(IEnumerable<IHostedService> services)
    {
        foreach (var hs in services) await hs.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SagaInstanceTtl_CreatesMongoTtlIndex()
    {
        var dbName = "saga_ttl_" + Guid.NewGuid().ToString("N");
        var (sp, bus, db) = BuildAndStart(dbName);
        var hosted = await StartAsync(sp);

        try
        {
            // Wait for hosted services (including SagaIndexesHostedService) to complete
            await Task.Delay(2000);

            var collection = db.GetCollection<TtlTestState>("bus_saga_ttl-test-state");
            using var cursor = await collection.Indexes.ListAsync();
            var indexList = await cursor.ToListAsync();

            var ttlIndex = indexList.FirstOrDefault(i =>
                i.Contains("name") && i["name"].AsString == "ix_ttl");

            ttlIndex.Should().NotBeNull("a TTL index named 'ix_ttl' should be created");

            var expireAfterSeconds = ttlIndex!["expireAfterSeconds"].ToInt64();
            expireAfterSeconds.Should().Be((long)TimeSpan.FromDays(1).TotalSeconds,
                "TTL should match the configured SagaInstanceTtl of 1 day");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task NoTtl_DoesNotCreateTtlIndex()
    {
        var dbName = "saga_nottl_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        // Register without TTL (default SagaInstanceTtl = TimeSpan.Zero)
        services.AddMongoBusSaga<TtlStateMachine, TtlTestState>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(2000);

            var collection = db.GetCollection<TtlTestState>("bus_saga_ttl-test-state");
            using var cursor = await collection.Indexes.ListAsync();
            var indexList = await cursor.ToListAsync();

            var ttlIndex = indexList.FirstOrDefault(i =>
                i.Contains("name") && i["name"].AsString == "ix_ttl");

            ttlIndex.Should().BeNull("no TTL index should be created when SagaInstanceTtl is zero");
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Changing_SagaInstanceTtl_Should_Update_The_Existing_Ttl_Index()
    {
        var dbName = "saga_ttl_change_" + Guid.NewGuid().ToString("N");
        var firstDeployment = await StartSagaBusAsync(dbName, opt => opt.SagaInstanceTtl = TimeSpan.FromDays(1));
        await firstDeployment.DisposeAsync();

        await using var secondDeployment = await StartSagaBusAsync(dbName, opt => opt.SagaInstanceTtl = TimeSpan.FromDays(2));

        (await TtlExpireAfterSecondsAsync(secondDeployment.Database, SagaCollectionName)).Should()
            .Be((long)TimeSpan.FromDays(2).TotalSeconds);
    }

    [Fact]
    public async Task Disabling_SagaInstanceTtl_Should_Remove_The_Ttl_Index()
    {
        var dbName = "saga_ttl_disable_" + Guid.NewGuid().ToString("N");
        var firstDeployment = await StartSagaBusAsync(dbName, opt => opt.SagaInstanceTtl = TimeSpan.FromDays(1));
        await firstDeployment.DisposeAsync();

        await using var secondDeployment = await StartSagaBusAsync(dbName, _ => { });

        (await TtlExpireAfterSecondsAsync(secondDeployment.Database, SagaCollectionName)).Should()
            .BeNull("saga instances must stop expiring once SagaInstanceTtl is turned off");
    }

    [Fact]
    public async Task Changing_HistoryTtl_Should_Update_The_Existing_Ttl_Index()
    {
        var dbName = "saga_history_ttl_change_" + Guid.NewGuid().ToString("N");
        var firstDeployment = await StartSagaBusAsync(dbName, opt =>
        {
            opt.HistoryEnabled = true;
            opt.HistoryTtl = TimeSpan.FromDays(7);
        });
        await firstDeployment.DisposeAsync();

        await using var secondDeployment = await StartSagaBusAsync(dbName, opt =>
        {
            opt.HistoryEnabled = true;
            opt.HistoryTtl = TimeSpan.FromDays(14);
        });

        (await TtlExpireAfterSecondsAsync(secondDeployment.Database, HistoryCollectionName)).Should()
            .Be((long)TimeSpan.FromDays(14).TotalSeconds);
    }

    private const string SagaCollectionName = "bus_saga_ttl-test-state";
    private const string HistoryCollectionName = "bus_saga_history_ttl-test-state";

    private Task<RunningBus> StartSagaBusAsync(string databaseName, Action<SagaOptions> configureSaga) =>
        RunningBus.StartAsync(
            fixture.ConnectionString,
            opt => opt.DatabaseName = databaseName,
            services => services.AddMongoBusSaga<TtlStateMachine, TtlTestState>(configure: configureSaga));

    private static async Task<long?> TtlExpireAfterSecondsAsync(IMongoDatabase db, string collectionName)
    {
        var indexes = await (await db.GetCollection<BsonDocument>(collectionName).Indexes.ListAsync()).ToListAsync();
        var ttlIndex = indexes.FirstOrDefault(index => index.Contains("expireAfterSeconds"));
        return ttlIndex?["expireAfterSeconds"].ToInt64();
    }
}
