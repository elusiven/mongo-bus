using Microsoft.Extensions.Hosting;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Bson;
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

        var inboxIndex = new CreateIndexModel<InboxMessage>(
            Builders<InboxMessage>.IndexKeys
                .Ascending(x => x.EndpointId)
                .Ascending(x => x.Status)
                .Ascending(x => x.VisibleUtc)
                .Ascending(x => x.LockedUntilUtc));

        var inboxCreatedIndex = new CreateIndexModel<InboxMessage>(
            Builders<InboxMessage>.IndexKeys
                .Ascending(x => x.CreatedUtc),
            new CreateIndexOptions { ExpireAfter = _options.ProcessedMessageTtl });

        var bindingUnique = new CreateIndexModel<Binding>(
            Builders<Binding>.IndexKeys.Ascending(x => x.Topic).Ascending(x => x.EndpointId),
            new CreateIndexOptions { Unique = true });

        await inbox.Indexes.CreateManyAsync(new[] { inboxIndex, inboxCreatedIndex }, cancellationToken: ct);
        await bindings.Indexes.CreateOneAsync(bindingUnique, cancellationToken: ct);

        // GridFS TTL (optional but recommended if using GridFS claim check)
        await CreateGridFsTtlIndexAsync(ct);
    }

    private async Task CreateGridFsTtlIndexAsync(CancellationToken ct)
    {
        // By default, GridFS uses 'fs.files' and 'fs.chunks'
        // Users can override bucket name, so we might have multiple buckets.
        // For simplicity, we'll try to apply to 'claimcheck.files' if it exists.
        // Better: apply to any collection ending in '.files' if it's manageable.
        
        // Let's just do it for common ones or let the user configure it?
        // Actually, we can list collections.
        using var cursor = await _db.ListCollectionNamesAsync(cancellationToken: ct);
        var collections = await cursor.ToListAsync(ct);
        foreach (var coll in collections.Where(c => c.EndsWith(".files")))
        {
            var filesColl = _db.GetCollection<BsonDocument>(coll);
            var ttlIndex = new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("uploadDate"),
                new CreateIndexOptions { ExpireAfter = _options.ProcessedMessageTtl.Add(TimeSpan.FromDays(1)) });
            
            await filesColl.Indexes.CreateOneAsync(ttlIndex, cancellationToken: ct);
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
