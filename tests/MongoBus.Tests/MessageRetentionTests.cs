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
public class MessageRetentionTests(MongoDbFixture fixture)
{
    private static readonly TimeSpan ShortRetention = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ExpiryTimeout = TimeSpan.FromSeconds(30);
    private static readonly DateTime MonthAgo = DateTime.UtcNow.AddDays(-30);
    private static readonly TimeSpan DefaultRetention = new MongoBusOptions().ProcessedMessageTtl;

    [Theory]
    [InlineData("Pending")]
    [InlineData("Dead")]
    public async Task Unprocessed_Inbox_Message_Should_Outlive_Retention_Window(string status)
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, NewDatabaseName(),
            opt => opt.ProcessedMessageTtl = ShortRetention);
        var inbox = bus.Database.GetCollection<InboxMessage>("bus_inbox");
        var processed = InboxMessageProcessedMonthAgo();
        var unprocessed = InboxMessageCreatedMonthAgo(status);
        await inbox.InsertManyAsync([processed, unprocessed]);

        await WaitUntilDeletedAsync(inbox, processed.Id);

        (await ExistsAsync(inbox, unprocessed.Id)).Should()
            .BeTrue($"a {status} message must not expire before it has been processed");
    }

    [Theory]
    [InlineData("Pending")]
    [InlineData("Dead")]
    public async Task Unpublished_Outbox_Message_Should_Outlive_Retention_Window(string status)
    {
        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, NewDatabaseName(), opt =>
        {
            opt.Outbox.Enabled = true;
            opt.Outbox.ProcessedMessageTtl = ShortRetention;
        });
        var outbox = bus.Database.GetCollection<OutboxMessage>("bus_outbox");
        var published = OutboxMessagePublishedMonthAgo();
        var unpublished = OutboxMessageCreatedMonthAgoDueTomorrow(status);
        await outbox.InsertManyAsync([published, unpublished]);

        await WaitUntilDeletedAsync(outbox, published.Id);

        (await ExistsAsync(outbox, unpublished.Id)).Should()
            .BeTrue($"a {status} outbox message must not expire before it has been published");
    }

    [Theory]
    [InlineData("bus_inbox")]
    [InlineData("bus_outbox")]
    public async Task Startup_Should_Remove_Legacy_CreatedUtc_Ttl_Index(string collectionName)
    {
        var databaseName = NewDatabaseName();
        var collection = Database(databaseName).GetCollection<BsonDocument>(collectionName);
        await CreateTtlIndexAsync(collection, "CreatedUtc", DefaultRetention);

        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, databaseName,
            opt => opt.Outbox.Enabled = true);

        (await TtlIndexesAsync(collection)).Should().NotContainKey("CreatedUtc");
    }

    [Fact]
    public async Task Changing_Retention_Window_Should_Update_Existing_Ttl_Index()
    {
        var databaseName = NewDatabaseName();
        var firstDeployment = await RunningBus.StartAsync(fixture.ConnectionString, databaseName,
            opt => opt.ProcessedMessageTtl = TimeSpan.FromDays(7));
        await firstDeployment.DisposeAsync();

        await using var secondDeployment = await RunningBus.StartAsync(fixture.ConnectionString, databaseName,
            opt => opt.ProcessedMessageTtl = TimeSpan.FromDays(14));

        var inbox = Database(databaseName).GetCollection<BsonDocument>("bus_inbox");
        (await TtlIndexesAsync(inbox)).Should().ContainKey("ProcessedUtc")
            .WhoseValue.Should().Be((long)TimeSpan.FromDays(14).TotalSeconds);
    }

    [Fact]
    public async Task Startup_Should_Not_Add_Ttl_Index_To_GridFs_Files_Collections()
    {
        var databaseName = NewDatabaseName();
        var files = Database(databaseName).GetCollection<BsonDocument>("user_uploads.files");
        await files.InsertOneAsync(new BsonDocument { ["filename"] = "avatar.png", ["uploadDate"] = DateTime.UtcNow });

        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, databaseName);

        (await TtlIndexesAsync(files)).Should().BeEmpty();
    }

    [Fact]
    public async Task Startup_Should_Remove_Legacy_GridFs_Ttl_Index_Created_By_MongoBus()
    {
        var databaseName = NewDatabaseName();
        var files = Database(databaseName).GetCollection<BsonDocument>("claimcheck.files");
        await CreateTtlIndexAsync(files, "uploadDate", DefaultRetention.Add(TimeSpan.FromDays(1)));

        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, databaseName);

        (await TtlIndexesAsync(files)).Should().BeEmpty();
    }

    [Fact]
    public async Task Startup_Should_Keep_User_Defined_GridFs_Ttl_Index()
    {
        var databaseName = NewDatabaseName();
        var files = Database(databaseName).GetCollection<BsonDocument>("media.files");
        await CreateTtlIndexAsync(files, "uploadDate", TimeSpan.FromHours(1));

        await using var bus = await RunningBus.StartAsync(fixture.ConnectionString, databaseName);

        (await TtlIndexesAsync(files)).Should().ContainKey("uploadDate")
            .WhoseValue.Should().Be((long)TimeSpan.FromHours(1).TotalSeconds);
    }

    private static string NewDatabaseName() => "retention_" + Guid.NewGuid().ToString("N");

    private IMongoDatabase Database(string databaseName) =>
        new MongoClient(fixture.ConnectionString).GetDatabase(databaseName);

    private static InboxMessage InboxMessageProcessedMonthAgo() => new()
    {
        EndpointId = "retention-endpoint",
        Topic = "retention.test",
        TypeId = "retention.test",
        PayloadJson = "{}",
        CreatedUtc = MonthAgo,
        VisibleUtc = MonthAgo,
        Status = "Processed",
        ProcessedUtc = MonthAgo
    };

    private static InboxMessage InboxMessageCreatedMonthAgo(string status) => new()
    {
        EndpointId = "retention-endpoint",
        Topic = "retention.test",
        TypeId = "retention.test",
        PayloadJson = "{}",
        CreatedUtc = MonthAgo,
        VisibleUtc = MonthAgo,
        Status = status
    };

    private static OutboxMessage OutboxMessagePublishedMonthAgo() => new()
    {
        Topic = "retention.test",
        TypeId = "retention.test",
        PayloadJson = "{}",
        CreatedUtc = MonthAgo,
        VisibleUtc = MonthAgo,
        Status = "Published",
        PublishedUtc = MonthAgo
    };

    private static OutboxMessage OutboxMessageCreatedMonthAgoDueTomorrow(string status) => new()
    {
        Topic = "retention.test",
        TypeId = "retention.test",
        PayloadJson = "{}",
        CreatedUtc = MonthAgo,
        VisibleUtc = DateTime.UtcNow.AddDays(1),
        Status = status
    };

    private static async Task CreateTtlIndexAsync(IMongoCollection<BsonDocument> collection, string field, TimeSpan expireAfter) =>
        await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(
            Builders<BsonDocument>.IndexKeys.Ascending(field),
            new CreateIndexOptions { ExpireAfter = expireAfter }));

    private static async Task<Dictionary<string, long>> TtlIndexesAsync(IMongoCollection<BsonDocument> collection)
    {
        var indexes = await (await collection.Indexes.ListAsync()).ToListAsync();
        return indexes
            .Where(index => index.Contains("expireAfterSeconds"))
            .ToDictionary(
                index => index["key"].AsBsonDocument.GetElement(0).Name,
                index => index["expireAfterSeconds"].ToInt64());
    }

    private static Task<bool> ExistsAsync<TDocument>(IMongoCollection<TDocument> collection, ObjectId id) =>
        collection.Find(Builders<TDocument>.Filter.Eq("_id", id)).AnyAsync();

    private static async Task WaitUntilDeletedAsync<TDocument>(IMongoCollection<TDocument> collection, ObjectId id)
    {
        var deadline = DateTime.UtcNow.Add(ExpiryTimeout);
        while (await ExistsAsync(collection, id))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"The TTL monitor did not remove document {id} within {ExpiryTimeout}.");

            await Task.Delay(250);
        }
    }

    private sealed class RunningBus(IReadOnlyList<IHostedService> hostedServices, IMongoDatabase database) : IAsyncDisposable
    {
        public IMongoDatabase Database { get; } = database;

        public static async Task<RunningBus> StartAsync(
            string connectionString,
            string databaseName,
            Action<MongoBusOptions>? configure = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddMongoBus(opt =>
            {
                opt.ConnectionString = connectionString;
                opt.DatabaseName = databaseName;
                configure?.Invoke(opt);
            });

            var provider = services.BuildServiceProvider();
            var started = new List<IHostedService>();
            try
            {
                foreach (var hostedService in provider.GetServices<IHostedService>())
                {
                    await hostedService.StartAsync(CancellationToken.None);
                    started.Add(hostedService);
                }
            }
            catch
            {
                await StopAllAsync(started);
                throw;
            }

            return new RunningBus(started, provider.GetRequiredService<IMongoDatabase>());
        }

        public async ValueTask DisposeAsync() => await StopAllAsync(hostedServices);

        private static async Task StopAllAsync(IEnumerable<IHostedService> services)
        {
            foreach (var hostedService in services.Reverse())
                await hostedService.StopAsync(CancellationToken.None);
        }
    }
}
