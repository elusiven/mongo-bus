using Microsoft.Extensions.Hosting;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

public sealed class MongoBusIndexesHostedService : IHostedService
{
    private readonly IMongoDatabase _db;
    private readonly MongoBusOptions _options;

    public MongoBusIndexesHostedService(IMongoDatabase db, MongoBusOptions options)
    {
        _db = db;
        _options = options;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var inbox = _db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var bindings = _db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);

        await inbox.Indexes.CreateManyAsync(BuildInboxIndexes(), cancellationToken: ct);
        await RetentionIndexes.DropLegacyMessageTtlIndexAsync(inbox, ct);
        await RetentionIndexes.EnsureAsync(inbox, nameof(InboxMessage.ProcessedUtc), _options.ProcessedMessageTtl, ct);

        // A claim outlives the processed message it stands for by nothing: both expire on the same schedule, so a
        // CloudEvent becomes claimable again exactly when the evidence that it was handled is gone.
        var idempotencyClaims = _db.GetCollection<InboxDedupRecord>(MongoBusConstants.InboxDedupCollectionName);
        await RetentionIndexes.EnsureAsync(idempotencyClaims, nameof(InboxDedupRecord.CreatedUtc), _options.ProcessedMessageTtl, ct);

        await bindings.Indexes.CreateOneAsync(BuildBindingIndex(), cancellationToken: ct);

        if (_options.Outbox.Enabled)
        {
            var outbox = _db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);
            await outbox.Indexes.CreateManyAsync(BuildOutboxIndexes(), cancellationToken: ct);
            await RetentionIndexes.DropLegacyMessageTtlIndexAsync(outbox, ct);
            await RetentionIndexes.EnsureAsync(outbox, nameof(OutboxMessage.PublishedUtc), _options.Outbox.ProcessedMessageTtl, ct);
        }

        await RetentionIndexes.DropLegacyGridFsTtlIndexesAsync(_db, _options.ProcessedMessageTtl, ct);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;

    private static IEnumerable<CreateIndexModel<InboxMessage>> BuildInboxIndexes()
    {
        var inboxIndex = new CreateIndexModel<InboxMessage>(
            Builders<InboxMessage>.IndexKeys
                .Ascending(x => x.EndpointId)
                .Ascending(x => x.Status)
                .Ascending(x => x.VisibleUtc)
                .Ascending(x => x.LockedUntilUtc));

        // Supports the per-consumer idempotency check (MongoBusRuntime.TrySkipIdempotentAsync)
        // and the outbox relay dedup query (MongoOutboxRelayService.BuildInboxMessagesAsync),
        // both of which filter by EndpointId + CloudEventId. Without it those run as collection
        // scans on every consumed/relayed message when idempotency is in play.
        var inboxDedupIndex = new CreateIndexModel<InboxMessage>(
            Builders<InboxMessage>.IndexKeys
                .Ascending(x => x.EndpointId)
                .Ascending(x => x.CloudEventId));

        return new[] { inboxIndex, inboxDedupIndex };
    }

    private static CreateIndexModel<Binding> BuildBindingIndex() =>
        new(
            Builders<Binding>.IndexKeys.Ascending(x => x.Topic).Ascending(x => x.EndpointId),
            new CreateIndexOptions { Unique = true });

    private static IEnumerable<CreateIndexModel<OutboxMessage>> BuildOutboxIndexes()
    {
        var relayIndex = new CreateIndexModel<OutboxMessage>(
            Builders<OutboxMessage>.IndexKeys
                .Ascending(x => x.Status)
                .Ascending(x => x.VisibleUtc)
                .Ascending(x => x.LockedUntilUtc));

        var cloudEventIndex = new CreateIndexModel<OutboxMessage>(
            Builders<OutboxMessage>.IndexKeys.Ascending(x => x.CloudEventId));

        return new[] { relayIndex, cloudEventIndex };
    }
}
