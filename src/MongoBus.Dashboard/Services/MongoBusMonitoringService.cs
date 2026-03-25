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
        var pendingTask = _inbox.CountDocumentsAsync(x => x.Status == "Pending", cancellationToken: ct);
        var processedTask = _inbox.CountDocumentsAsync(x => x.Status == "Processed", cancellationToken: ct);
        var deadTask = _inbox.CountDocumentsAsync(x => x.Status == "Dead", cancellationToken: ct);

        var recentFailuresTask = _inbox.Find(x => x.Status == "Dead")
            .SortByDescending(x => x.CreatedUtc)
            .Limit(10)
            .Project(x => new RecentFailure(
                x.Id.ToString(),
                x.EndpointId,
                x.TypeId,
                x.LastError ?? "Unknown error",
                x.CreatedUtc))
            .ToListAsync(ct);

        var endpointStatsTask = _inbox.Aggregate()
            .Group(x => x.EndpointId, g => new
            {
                EndpointId = g.Key,
                Pending = g.Count(x => x.Status == "Pending"),
                Processed = g.Count(x => x.Status == "Processed"),
                Dead = g.Count(x => x.Status == "Dead")
            })
            .Project(x => new EndpointStats(x.EndpointId, x.Pending, x.Processed, x.Dead))
            .ToListAsync(ct);

        await Task.WhenAll(pendingTask, processedTask, deadTask, recentFailuresTask, endpointStatsTask);

        return new DashboardStats(
            await pendingTask,
            await processedTask,
            await deadTask,
            await recentFailuresTask,
            await endpointStatsTask);
    }

    public async Task<IReadOnlyList<string>> GetSagaCollectionsAsync(CancellationToken ct = default)
    {
        var filter = new BsonDocumentFilterDefinition<BsonDocument>(
            new BsonDocument("name", new BsonDocument("$regex", $"^{SagaCollectionPrefix}(?!history_)")));
        using var cursor = await db.ListCollectionNamesAsync(new ListCollectionNamesOptions { Filter = filter }, ct);
        return await cursor.ToListAsync(ct);
    }

    public async Task<SagaDashboardStats> GetSagaStatsAsync(string collectionName, CancellationToken ct = default)
    {
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
        var collection = db.GetCollection<BsonDocument>(collectionName);
        var filter = string.IsNullOrEmpty(stateFilter)
            ? FilterDefinition<BsonDocument>.Empty
            : Builders<BsonDocument>.Filter.Eq("CurrentState", stateFilter);

        return await collection.Find(filter)
            .SortByDescending(x => x["LastModifiedUtc"])
            .Skip(skip)
            .Limit(take)
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<SagaHistoryEntry>> GetSagaHistoryAsync(
        string historyCollectionName, string correlationId, CancellationToken ct = default)
    {
        var collection = db.GetCollection<SagaHistoryEntry>(historyCollectionName);
        return await collection.Find(x => x.CorrelationId == correlationId)
            .SortBy(x => x.TimestampUtc)
            .ToListAsync(ct);
    }
}
