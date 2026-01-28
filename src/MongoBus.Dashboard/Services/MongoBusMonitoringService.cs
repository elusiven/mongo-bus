using MongoBus.Infrastructure;
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

public interface IMongoBusMonitoringService
{
    Task<DashboardStats> GetStatsAsync(CancellationToken ct = default);
}

public sealed class MongoBusMonitoringService(IMongoDatabase db) : IMongoBusMonitoringService
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

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
}
