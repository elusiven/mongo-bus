using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

internal sealed class SagaIndexesHostedService<TInstance>(
    IMongoDatabase db,
    string collectionName,
    SagaOptions options,
    ILogger<SagaIndexesHostedService<TInstance>> log)
    : IHostedService
    where TInstance : class, ISagaInstance
{
    private const string TtlIndexName = "ix_ttl";

    public async Task StartAsync(CancellationToken ct)
    {
        try
        {
            await EnsureInstanceIndexesAsync(ct);

            if (options.HistoryEnabled)
                await EnsureHistoryIndexesAsync(ct);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Failed to create indexes on saga collection '{Collection}'", collectionName);
            throw;
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;

    private async Task EnsureInstanceIndexesAsync(CancellationToken ct)
    {
        var collection = db.GetCollection<TInstance>(collectionName);

        await collection.Indexes.CreateManyAsync(
        [
            new CreateIndexModel<TInstance>(
                Builders<TInstance>.IndexKeys.Ascending(x => x.CorrelationId),
                new CreateIndexOptions { Unique = true, Name = "ix_correlation_id" }),
            new CreateIndexModel<TInstance>(
                Builders<TInstance>.IndexKeys.Ascending(x => x.CurrentState),
                new CreateIndexOptions { Name = "ix_current_state" })
        ], ct);

        // The TTL index deletes saga instances, so turning SagaInstanceTtl off must remove it, not just stop creating it.
        if (options.SagaInstanceTtl > TimeSpan.Zero)
            await RetentionIndexes.EnsureNamedAsync(collection, TtlIndexName, nameof(ISagaInstance.LastModifiedUtc), options.SagaInstanceTtl, ct);
        else
            await RetentionIndexes.DropAsync(collection, nameof(ISagaInstance.LastModifiedUtc), ct);

        log.LogInformation("Ensured indexes on saga collection '{Collection}'", collectionName);
    }

    private async Task EnsureHistoryIndexesAsync(CancellationToken ct)
    {
        var historyCollectionName = $"bus_saga_history_{EndpointNameHelper.FromConsumerType(typeof(TInstance))}";
        var historyCollection = db.GetCollection<SagaHistoryEntry>(historyCollectionName);

        await historyCollection.Indexes.CreateOneAsync(
            new CreateIndexModel<SagaHistoryEntry>(
                Builders<SagaHistoryEntry>.IndexKeys.Ascending(x => x.CorrelationId),
                new CreateIndexOptions { Name = "ix_correlation_id" }),
            cancellationToken: ct);

        await RetentionIndexes.EnsureNamedAsync(historyCollection, TtlIndexName, nameof(SagaHistoryEntry.TimestampUtc), options.HistoryTtl, ct);

        log.LogInformation("Ensured indexes on saga history collection '{Collection}'", historyCollectionName);
    }
}
