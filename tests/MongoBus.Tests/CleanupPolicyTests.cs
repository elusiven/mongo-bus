using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class CleanupPolicyTests(MongoDbFixture fixture)
{
    [Fact]
    public async Task CustomTtl_ShouldBeAppliedToInboxIndex()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        var customTtl = TimeSpan.FromHours(24);
        var dbName = "cleanup_test_" + Guid.NewGuid().ToString("N");
        
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
            opt.ProcessedMessageTtl = customTtl;
        });

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();

        // Act
        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            // Assert
            var inbox = db.GetCollection<InboxMessage>("bus_inbox");
            var indexes = await (await inbox.Indexes.ListAsync()).ToListAsync();
            
            // Find the TTL index on CreatedUtc
            var ttlIndex = indexes.FirstOrDefault(idx => 
                idx.Contains("expireAfterSeconds") && 
                idx["key"].AsBsonDocument.Contains("CreatedUtc"));

            ttlIndex.Should().NotBeNull("TTL index on CreatedUtc should exist");
            ttlIndex!["expireAfterSeconds"].ToDouble().Should().Be(customTtl.TotalSeconds);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
