using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaHistoryTests(MongoDbFixture fixture)
{
    public sealed class HistoryOrderSubmitted
    {
        public string OrderId { get; set; } = "";
    }

    public sealed class HistoryOrderAccepted
    {
        public string OrderId { get; set; } = "";
    }

    public sealed class HistoryState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public class HistoryStateMachine : MongoBusStateMachine<HistoryState>
    {
        public SagaState Submitted { get; private set; }
        public SagaState Accepted { get; private set; }

        public SagaEvent<HistoryOrderSubmitted> OrderSubmittedEvent { get; private set; }
        public SagaEvent<HistoryOrderAccepted> OrderAcceptedEvent { get; private set; }

        public HistoryStateMachine()
        {
            Event(() => OrderSubmittedEvent, "saga.test.hist.submitted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));
            Event(() => OrderAcceptedEvent, "saga.test.hist.accepted", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(OrderSubmittedEvent).TransitionTo(Submitted));

            During(Submitted,
                When(OrderAcceptedEvent).TransitionTo(Accepted));
        }
    }

    [Fact]
    public async Task HistoryEnabled_RecordsStateTransitions()
    {
        var dbName = "saga_history_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<HistoryStateMachine, HistoryState>(opt =>
        {
            opt.HistoryEnabled = true;
            opt.HistoryTtl = TimeSpan.FromDays(7);
        });

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout &&
                   await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < 2)
                await Task.Delay(100);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.hist.submitted",
                new HistoryOrderSubmitted { OrderId = "H-1" },
                correlationId: correlationId);

            // Wait for saga to reach Submitted state
            var sagaCollection = db.GetCollection<HistoryState>("bus_saga_history-state");
            var sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await sagaCollection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "Submitted") break;
                await Task.Delay(100);
            }

            await bus.PublishAsync("saga.test.hist.accepted",
                new HistoryOrderAccepted { OrderId = "H-1" },
                correlationId: correlationId);

            // Wait for Accepted state
            sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await sagaCollection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "Accepted") break;
                await Task.Delay(100);
            }

            // Check history entries
            var historyCollection = db.GetCollection<SagaHistoryEntry>("bus_saga_history_history-state");
            var entries = await historyCollection
                .Find(x => x.CorrelationId == correlationId)
                .SortBy(x => x.TimestampUtc)
                .ToListAsync();

            entries.Should().HaveCount(2);

            entries[0].PreviousState.Should().Be("Initial");
            entries[0].NewState.Should().Be("Submitted");
            entries[0].EventTypeId.Should().Be("saga.test.hist.submitted");
            entries[0].VersionAfter.Should().Be(1);

            entries[1].PreviousState.Should().Be("Submitted");
            entries[1].NewState.Should().Be("Accepted");
            entries[1].EventTypeId.Should().Be("saga.test.hist.accepted");
            entries[1].VersionAfter.Should().Be(2);
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task HistoryDisabled_NoHistoryWritten()
    {
        var dbName = "saga_no_history_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        // HistoryEnabled defaults to false
        services.AddMongoBusSaga<HistoryStateMachine, HistoryState>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout &&
                   await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < 2)
                await Task.Delay(100);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.hist.submitted",
                new HistoryOrderSubmitted { OrderId = "H-2" },
                correlationId: correlationId);

            // Wait for saga state
            var sagaCollection = db.GetCollection<HistoryState>("bus_saga_history-state");
            var sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await sagaCollection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "Submitted") break;
                await Task.Delay(100);
            }

            // History collection should not exist or be empty
            var historyCollection = db.GetCollection<SagaHistoryEntry>("bus_saga_history_history-state");
            var count = await historyCollection.CountDocumentsAsync(FilterDefinition<SagaHistoryEntry>.Empty);
            count.Should().Be(0);
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task HistoryTtl_CreatesTtlIndex()
    {
        var dbName = "saga_hist_ttl_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<HistoryStateMachine, HistoryState>(opt =>
        {
            opt.HistoryEnabled = true;
            opt.HistoryTtl = TimeSpan.FromDays(7);
        });

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(1000);

            var historyCollection = db.GetCollection<SagaHistoryEntry>("bus_saga_history_history-state");
            using var cursor = await historyCollection.Indexes.ListAsync();
            var indexList = await cursor.ToListAsync();

            var ttlIndex = indexList.FirstOrDefault(i =>
                i.Contains("name") && i["name"].AsString == "ix_ttl");

            ttlIndex.Should().NotBeNull("a TTL index should be created on the history collection");

            var expireAfterSeconds = ttlIndex!["expireAfterSeconds"].ToInt64();
            expireAfterSeconds.Should().Be((long)TimeSpan.FromDays(7).TotalSeconds);
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }
}
