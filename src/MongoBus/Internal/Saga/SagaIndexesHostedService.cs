using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
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
    public async Task StartAsync(CancellationToken ct)
    {
        try
        {
            var collection = db.GetCollection<TInstance>(collectionName);

            var indexes = new List<CreateIndexModel<TInstance>>
            {
                new(
                    Builders<TInstance>.IndexKeys.Ascending(x => x.CorrelationId),
                    new CreateIndexOptions { Unique = true, Name = "ix_correlation_id" }),
                new(
                    Builders<TInstance>.IndexKeys.Ascending(x => x.CurrentState),
                    new CreateIndexOptions { Name = "ix_current_state" })
            };

            if (options.SagaInstanceTtl > TimeSpan.Zero)
            {
                indexes.Add(new CreateIndexModel<TInstance>(
                    Builders<TInstance>.IndexKeys.Ascending(x => x.LastModifiedUtc),
                    new CreateIndexOptions
                    {
                        Name = "ix_ttl",
                        ExpireAfter = options.SagaInstanceTtl
                    }));
            }

            await collection.Indexes.CreateManyAsync(indexes, ct);

            log.LogInformation(
                "Created {Count} indexes on saga collection '{Collection}'",
                indexes.Count, collectionName);

            // Create history collection indexes if history is enabled
            if (options.HistoryEnabled)
            {
                var historyCollectionName = $"bus_saga_history_{MongoBus.Utils.EndpointNameHelper.FromConsumerType(typeof(TInstance))}";
                var historyCollection = db.GetCollection<MongoBus.Models.Saga.SagaHistoryEntry>(historyCollectionName);

                var historyIndexes = new List<CreateIndexModel<MongoBus.Models.Saga.SagaHistoryEntry>>
                {
                    new(
                        Builders<MongoBus.Models.Saga.SagaHistoryEntry>.IndexKeys.Ascending(x => x.CorrelationId),
                        new CreateIndexOptions { Name = "ix_correlation_id" }),
                    new(
                        Builders<MongoBus.Models.Saga.SagaHistoryEntry>.IndexKeys.Ascending(x => x.TimestampUtc),
                        new CreateIndexOptions
                        {
                            Name = "ix_ttl",
                            ExpireAfter = options.HistoryTtl
                        })
                };

                await historyCollection.Indexes.CreateManyAsync(historyIndexes, ct);

                log.LogInformation(
                    "Created {Count} indexes on saga history collection '{Collection}'",
                    historyIndexes.Count, historyCollectionName);
            }
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Failed to create indexes on saga collection '{Collection}'", collectionName);
            throw;
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
