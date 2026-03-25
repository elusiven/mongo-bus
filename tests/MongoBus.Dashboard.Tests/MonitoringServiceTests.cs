using FluentAssertions;
using MongoBus.Dashboard.Services;
using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoBus.Tests;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Dashboard.Tests;

[CollectionDefinition("Mongo collection")]
public class MongoCollection : ICollectionFixture<MongoDbFixture> { }

[Collection("Mongo collection")]
public class MonitoringServiceTests(MongoDbFixture fixture)
{
    [Fact]
    public async Task GetStatsAsync_ShouldReturnCorrectAggregations()
    {
        // Arrange
        var client = new MongoClient(fixture.ConnectionString);
        var dbName = "monitoring_test_" + Guid.NewGuid().ToString("N");
        var db = client.GetDatabase(dbName);
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

        var now = DateTime.UtcNow;
        var messages = new List<InboxMessage>
        {
            new() { EndpointId = "e1", Status = "Pending", CreatedUtc = now, Topic = "t1", TypeId = "t1", PayloadJson = "{}" },
            new() { EndpointId = "e1", Status = "Processed", CreatedUtc = now, Topic = "t1", TypeId = "t1", PayloadJson = "{}" },
            new() { EndpointId = "e1", Status = "Dead", LastError = "Error 1", CreatedUtc = now, Topic = "t1", TypeId = "t1", PayloadJson = "{}" },
            new() { EndpointId = "e2", Status = "Pending", CreatedUtc = now, Topic = "t2", TypeId = "t2", PayloadJson = "{}" }
        };
        await inbox.InsertManyAsync(messages);

        var service = new MongoBusMonitoringService(db);

        // Act
        var stats = await service.GetStatsAsync();

        // Assert
        stats.PendingCount.Should().Be(2);
        stats.ProcessedCount.Should().Be(1);
        stats.DeadCount.Should().Be(1);

        stats.RecentFailures.Should().HaveCount(1);
        stats.RecentFailures[0].Error.Should().Be("Error 1");
        stats.RecentFailures[0].EndpointId.Should().Be("e1");

        stats.Endpoints.Should().HaveCount(2);
        var e1 = stats.Endpoints.Single(x => x.EndpointId == "e1");
        e1.Pending.Should().Be(1);
        e1.Processed.Should().Be(1);
        e1.Dead.Should().Be(1);

        var e2 = stats.Endpoints.Single(x => x.EndpointId == "e2");
        e2.Pending.Should().Be(1);
        e2.Processed.Should().Be(0);
        e2.Dead.Should().Be(0);
    }

    [Fact]
    public async Task GetSagaCollectionsAsync_ShouldReturnSagaCollections()
    {
        var client = new MongoClient(fixture.ConnectionString);
        var dbName = "saga_collections_test_" + Guid.NewGuid().ToString("N");
        var db = client.GetDatabase(dbName);

        // Insert a document to create the collection
        var sagaCollection = db.GetCollection<BsonDocument>("bus_saga_order-state");
        await sagaCollection.InsertOneAsync(new BsonDocument("test", true));

        // Also create a history collection (should be excluded)
        var historyCollection = db.GetCollection<BsonDocument>("bus_saga_history_order-state");
        await historyCollection.InsertOneAsync(new BsonDocument("test", true));

        var service = new MongoBusMonitoringService(db);
        var collections = await service.GetSagaCollectionsAsync();

        collections.Should().Contain("bus_saga_order-state");
        collections.Should().NotContain("bus_saga_history_order-state");
    }

    [Fact]
    public async Task GetSagaStatsAsync_ShouldReturnStateBreakdown()
    {
        var client = new MongoClient(fixture.ConnectionString);
        var dbName = "saga_stats_test_" + Guid.NewGuid().ToString("N");
        var db = client.GetDatabase(dbName);

        var collection = db.GetCollection<BsonDocument>("bus_saga_test-state");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { ["CorrelationId"] = "1", ["CurrentState"] = "Processing", ["Version"] = 1 },
            new BsonDocument { ["CorrelationId"] = "2", ["CurrentState"] = "Processing", ["Version"] = 1 },
            new BsonDocument { ["CorrelationId"] = "3", ["CurrentState"] = "Completed", ["Version"] = 2 }
        });

        var service = new MongoBusMonitoringService(db);
        var stats = await service.GetSagaStatsAsync("bus_saga_test-state");

        stats.TotalInstances.Should().Be(3);
        stats.ByState.Should().HaveCount(2);
        stats.ByState.Single(s => s.State == "Processing").Count.Should().Be(2);
        stats.ByState.Single(s => s.State == "Completed").Count.Should().Be(1);
    }

    [Fact]
    public async Task GetSagaHistoryAsync_ShouldReturnEntriesForCorrelationId()
    {
        var client = new MongoClient(fixture.ConnectionString);
        var dbName = "saga_history_test_" + Guid.NewGuid().ToString("N");
        var db = client.GetDatabase(dbName);

        var historyCollection = db.GetCollection<SagaHistoryEntry>("bus_saga_history_test-state");
        var correlationId = Guid.NewGuid().ToString("N");
        await historyCollection.InsertManyAsync(new[]
        {
            new SagaHistoryEntry
            {
                CorrelationId = correlationId, SagaType = "TestState",
                PreviousState = "Initial", NewState = "Processing",
                EventTypeId = "test.started", TimestampUtc = DateTime.UtcNow.AddSeconds(-2), VersionAfter = 1
            },
            new SagaHistoryEntry
            {
                CorrelationId = correlationId, SagaType = "TestState",
                PreviousState = "Processing", NewState = "Completed",
                EventTypeId = "test.completed", TimestampUtc = DateTime.UtcNow, VersionAfter = 2
            },
            new SagaHistoryEntry
            {
                CorrelationId = "other-id", SagaType = "TestState",
                PreviousState = "Initial", NewState = "Processing",
                EventTypeId = "test.started", TimestampUtc = DateTime.UtcNow, VersionAfter = 1
            }
        });

        var service = new MongoBusMonitoringService(db);
        var entries = await service.GetSagaHistoryAsync("bus_saga_history_test-state", correlationId);

        entries.Should().HaveCount(2);
        entries[0].PreviousState.Should().Be("Initial");
        entries[0].NewState.Should().Be("Processing");
        entries[1].PreviousState.Should().Be("Processing");
        entries[1].NewState.Should().Be("Completed");
    }
}
