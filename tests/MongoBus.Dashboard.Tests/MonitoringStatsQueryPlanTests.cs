using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Dashboard.Services;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Tests;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Dashboard.Tests;

[Collection("Mongo collection")]
public class MonitoringStatsQueryPlanTests(MongoDbFixture fixture)
{
    [Fact]
    public async Task GetStatsAsync_Should_Not_Scan_The_Whole_Inbox()
    {
        var databaseName = "stats_query_plans_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(databaseName);
        var hostedServices = await StartAsync(services);
        try
        {
            var db = services.GetRequiredService<IMongoDatabase>();
            await SeedInboxAsync(db);
            await SetProfilingLevelAsync(db, level: 2);

            using (var scope = services.CreateScope())
                await scope.ServiceProvider.GetRequiredService<IMongoBusMonitoringService>().GetStatsAsync();

            await SetProfilingLevelAsync(db, level: 0);
            var inboxQueryPlans = await InboxQueryPlansAsync(db);
            inboxQueryPlans.Should().NotBeEmpty();
            inboxQueryPlans.Should().NotContain(plan => plan.Contains("COLLSCAN"),
                "the dashboard polls these queries every few seconds, so they must not read every inbox document");
        }
        finally
        {
            foreach (var hostedService in hostedServices)
                await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private ServiceProvider BuildServices(string databaseName)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = databaseName;
        });
        services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);
        return services.BuildServiceProvider();
    }

    private static async Task<List<IHostedService>> StartAsync(IServiceProvider services)
    {
        var hostedServices = services.GetServices<IHostedService>().ToList();
        foreach (var hostedService in hostedServices)
            await hostedService.StartAsync(CancellationToken.None);
        return hostedServices;
    }

    private static Task SeedInboxAsync(IMongoDatabase db)
    {
        var now = DateTime.UtcNow;
        InboxMessage Message(string endpointId, string status) => new()
        {
            EndpointId = endpointId,
            Status = status,
            Topic = "stats.test",
            TypeId = "stats.test",
            PayloadJson = "{}",
            CreatedUtc = now,
            VisibleUtc = now
        };

        return db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName).InsertManyAsync(
        [
            Message("orders", "Pending"),
            Message("orders", "Processed"),
            Message("orders", "Dead"),
            Message("billing", "Pending")
        ]);
    }

    private static Task SetProfilingLevelAsync(IMongoDatabase db, int level) =>
        db.RunCommandAsync<BsonDocument>(new BsonDocument("profile", level));

    private static async Task<List<string>> InboxQueryPlansAsync(IMongoDatabase db)
    {
        var inboxNamespace = $"{db.DatabaseNamespace.DatabaseName}.{MongoBusConstants.InboxCollectionName}";
        var profiledInboxQueries = Builders<BsonDocument>.Filter.Eq("ns", inboxNamespace)
            & Builders<BsonDocument>.Filter.Exists("planSummary");

        var entries = await db.GetCollection<BsonDocument>("system.profile").Find(profiledInboxQueries).ToListAsync();
        return entries.Select(entry => entry["planSummary"].AsString).ToList();
    }
}
