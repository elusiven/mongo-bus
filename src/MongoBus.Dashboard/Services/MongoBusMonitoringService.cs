using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Dashboard.Services;

public sealed record DashboardStats(
    long PendingCount,
    long ProcessedCount,
    long DeadCount,
    IReadOnlyList<RecentFailure> RecentFailures,
    IReadOnlyList<EndpointStats> Endpoints);

public sealed record RecentFailure(
    string Id,
    string EndpointId,
    string TypeId,
    string Error,
    DateTime Timestamp);

public sealed record EndpointStats(
    string EndpointId,
    long Pending,
    long Processed,
    long Dead);

public sealed record SagaDashboardStats(
    string CollectionName,
    long TotalInstances,
    IReadOnlyList<SagaStateCount> ByState);

public sealed record SagaStateCount(string State, long Count);

public interface IMongoBusMonitoringService
{
    Task<DashboardStats> GetStatsAsync(CancellationToken ct = default);
    Task<IReadOnlyList<string>> GetSagaCollectionsAsync(CancellationToken ct = default);
    Task<SagaDashboardStats> GetSagaStatsAsync(string collectionName, CancellationToken ct = default);
    Task<IReadOnlyList<BsonDocument>> GetSagaInstancesAsync(string collectionName, string? stateFilter, int skip, int take, CancellationToken ct = default);
    Task<IReadOnlyList<SagaHistoryEntry>> GetSagaHistoryAsync(string historyCollectionName, string correlationId, CancellationToken ct = default);
}

public sealed class MongoBusMonitoringService(IMongoDatabase db) : IMongoBusMonitoringService
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
    private const string SagaCollectionPrefix = "bus_saga_";
    private const string SagaHistoryPrefix = "bus_saga_history_";

    public async Task<DashboardStats> GetStatsAsync(CancellationToken ct = default)
    {
        var endpointStatsTask = CountMessagesPerEndpointAsync(ct);
        var recentFailuresTask = FindRecentFailuresAsync(ct);
        await Task.WhenAll(endpointStatsTask, recentFailuresTask);

        var endpointStats = await endpointStatsTask;
        return new DashboardStats(
            endpointStats.Sum(e => e.Pending),
            endpointStats.Sum(e => e.Processed),
            endpointStats.Sum(e => e.Dead),
            await recentFailuresTask,
            endpointStats);
    }

    /// <summary>
    /// Counts messages per endpoint and status in one pass. Sorting on the fields of the bus's
    /// (EndpointId, Status, ...) inbox index first lets MongoDB answer from that index instead of reading every
    /// inbox document; the dashboard polls this every few seconds.
    /// </summary>
    private async Task<IReadOnlyList<EndpointStats>> CountMessagesPerEndpointAsync(CancellationToken ct)
    {
        var counts = await _inbox.Aggregate()
            .SortBy(x => x.EndpointId)
            .ThenBy(x => x.Status)
            .Group(x => new { x.EndpointId, x.Status }, g => new { g.Key.EndpointId, g.Key.Status, Count = g.LongCount() })
            .ToListAsync(ct);

        return counts
            .GroupBy(count => count.EndpointId)
            .Select(endpoint => new EndpointStats(
                endpoint.Key,
                endpoint.Where(c => c.Status == "Pending").Sum(c => c.Count),
                endpoint.Where(c => c.Status == "Processed").Sum(c => c.Count),
                endpoint.Where(c => c.Status == "Dead").Sum(c => c.Count)))
            .ToList();
    }

    private async Task<IReadOnlyList<RecentFailure>> FindRecentFailuresAsync(CancellationToken ct) =>
        await _inbox.Find(x => x.Status == "Dead")
            .SortByDescending(x => x.CreatedUtc)
            .Limit(10)
            .Project(x => new RecentFailure(
                x.Id.ToString(),
                x.EndpointId,
                x.TypeId,
                x.LastError ?? "Unknown error",
                x.CreatedUtc))
            .ToListAsync(ct);

    public async Task<IReadOnlyList<string>> GetSagaCollectionsAsync(CancellationToken ct = default)
    {
        var filter = new BsonDocumentFilterDefinition<BsonDocument>(
            new BsonDocument("name", new BsonDocument("$regex", $"^{SagaCollectionPrefix}(?!history_)")));
        using var cursor = await db.ListCollectionNamesAsync(new ListCollectionNamesOptions { Filter = filter }, ct);
        return await cursor.ToListAsync(ct);
    }

    public async Task<SagaDashboardStats> GetSagaStatsAsync(string collectionName, CancellationToken ct = default)
    {
        EnsureSagaCollection(collectionName);
        var collection = db.GetCollection<BsonDocument>(collectionName);
        var total = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: ct);

        var groupStage = new BsonDocument("$group", new BsonDocument
        {
            { "_id", "$CurrentState" },
            { "Count", new BsonDocument("$sum", 1) }
        });

        var projectStage = new BsonDocument("$project", new BsonDocument
        {
            { "State", "$_id" },
            { "Count", 1 },
            { "_id", 0 }
        });

        var results = await collection.Aggregate()
            .AppendStage<BsonDocument>(groupStage)
            .AppendStage<BsonDocument>(projectStage)
            .ToListAsync(ct);

        var byState = results.Select(r =>
            new SagaStateCount(r["State"].AsString, r["Count"].ToInt64())).ToList();

        return new SagaDashboardStats(collectionName, total, byState);
    }

    public async Task<IReadOnlyList<BsonDocument>> GetSagaInstancesAsync(
        string collectionName, string? stateFilter, int skip, int take, CancellationToken ct = default)
    {
        EnsureSagaCollection(collectionName);
        var collection = db.GetCollection<BsonDocument>(collectionName);
        var filter = string.IsNullOrEmpty(stateFilter)
            ? FilterDefinition<BsonDocument>.Empty
            : Builders<BsonDocument>.Filter.Eq("CurrentState", stateFilter);

        // Saga instances can hold any application data; only the fields the dashboard shows leave the database.
        var summaryFields = Builders<BsonDocument>.Projection
            .Include("CorrelationId")
            .Include("CurrentState")
            .Include("Version")
            .Include("CreatedUtc")
            .Include("LastModifiedUtc")
            .Exclude("_id");

        return await collection.Find(filter)
            .SortByDescending(x => x["LastModifiedUtc"])
            .Skip(skip)
            .Limit(take)
            .Project(summaryFields)
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<SagaHistoryEntry>> GetSagaHistoryAsync(
        string historyCollectionName, string correlationId, CancellationToken ct = default)
    {
        EnsureSagaHistoryCollection(historyCollectionName);
        var collection = db.GetCollection<SagaHistoryEntry>(historyCollectionName);
        return await collection.Find(x => x.CorrelationId == correlationId)
            .SortBy(x => x.TimestampUtc)
            .ToListAsync(ct);
    }

    // Collection names arrive from untrusted route parameters. Without this guard the
    // dashboard could be coerced into reading any collection in the database (e.g.
    // bus_inbox message payloads). Only saga collections owned by the bus are addressable.
    private static void EnsureSagaCollection(string collectionName)
    {
        if (!IsValidSagaCollectionName(collectionName) ||
            !collectionName.StartsWith(SagaCollectionPrefix, StringComparison.Ordinal))
            throw new ArgumentException(
                $"'{collectionName}' is not a valid saga collection name.", nameof(collectionName));
    }

    private static void EnsureSagaHistoryCollection(string collectionName)
    {
        if (!IsValidSagaCollectionName(collectionName) ||
            !collectionName.StartsWith(SagaHistoryPrefix, StringComparison.Ordinal))
            throw new ArgumentException(
                $"'{collectionName}' is not a valid saga history collection name.", nameof(collectionName));
    }

    private static bool IsValidSagaCollectionName(string collectionName) =>
        !string.IsNullOrEmpty(collectionName) &&
        collectionName.IndexOfAny(['$', '\0']) < 0;
}
