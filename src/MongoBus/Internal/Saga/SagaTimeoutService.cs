using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Utils;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

internal sealed class SagaTimeoutService<TInstance>(
    IMongoDatabase db,
    SagaOptions options,
    SagaHistoryWriter<TInstance>? historyWriter,
    ILogger<SagaTimeoutService<TInstance>> logger)
    : BackgroundService
    where TInstance : class, ISagaInstance
{
    private readonly IMongoCollection<TInstance> _collection =
        db.GetCollection<TInstance>($"bus_saga_{EndpointNameHelper.FromConsumerType(typeof(TInstance))}");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ScanForExpiredSagasAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error scanning for expired saga instances");
            }

            await Task.Delay(options.TimeoutScanInterval, stoppingToken);
        }
    }

    private async Task ScanForExpiredSagasAsync(CancellationToken ct)
    {
        var cutoff = DateTime.UtcNow.Subtract(options.SagaTimeout);
        var timeoutState = options.TimeoutStateName;

        var filter = Builders<TInstance>.Filter.And(
            Builders<TInstance>.Filter.Lt(x => x.CreatedUtc, cutoff),
            Builders<TInstance>.Filter.Ne(x => x.CurrentState, "Final"),
            Builders<TInstance>.Filter.Ne(x => x.CurrentState, timeoutState));

        using var cursor = await _collection.FindAsync(filter, cancellationToken: ct);
        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var instance in cursor.Current)
            {
                await TimeoutInstanceAsync(instance, timeoutState, ct);
            }
        }
    }

    private async Task TimeoutInstanceAsync(TInstance instance, string timeoutState, CancellationToken ct)
    {
        var previousState = instance.CurrentState;
        var previousVersion = instance.Version;

        var filter = Builders<TInstance>.Filter.And(
            Builders<TInstance>.Filter.Eq(x => x.CorrelationId, instance.CorrelationId),
            Builders<TInstance>.Filter.Eq(x => x.Version, previousVersion));

        var update = Builders<TInstance>.Update
            .Set(x => x.CurrentState, timeoutState)
            .Set(x => x.LastModifiedUtc, DateTime.UtcNow)
            .Inc(x => x.Version, 1);

        var result = await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);

        if (result.ModifiedCount > 0)
        {
            logger.LogInformation(
                "Saga {CorrelationId} timed out: {PreviousState} -> {TimeoutState}",
                instance.CorrelationId, previousState, timeoutState);

            if (historyWriter != null)
            {
                var dummyContext = new Models.ConsumeContext(
                    EndpointId: "saga-timeout",
                    TypeId: "__timeout__",
                    MessageId: ObjectId.Empty,
                    Attempt: 0,
                    Subject: null,
                    Source: "saga-timeout-service",
                    CloudEventId: Guid.NewGuid().ToString("N"));

                await historyWriter.WriteAsync(
                    instance.CorrelationId, previousState, timeoutState,
                    "__timeout__", dummyContext,
                    previousVersion + 1, ct);
            }
        }
    }
}
