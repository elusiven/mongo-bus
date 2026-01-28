using FluentAssertions;
using MongoBus.Dashboard.Services;
using MongoBus.Infrastructure;
using MongoBus.Tests;
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
}
