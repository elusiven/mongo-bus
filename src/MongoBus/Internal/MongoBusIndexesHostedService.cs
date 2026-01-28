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
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
