using Microsoft.Extensions.Hosting;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Dashboard.Services;

/// <summary>
/// Creates the inbox index the dashboard's dead-letter list needs. It is owned by the dashboard rather than the core
/// bus so that applications without the dashboard do not pay for maintaining it on every inbox write.
/// </summary>
internal sealed class DashboardIndexesHostedService(IMongoDatabase db) : IHostedService
{
    public Task StartAsync(CancellationToken ct)
    {
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadLettersByAge = new CreateIndexModel<InboxMessage>(
            Builders<InboxMessage>.IndexKeys
                .Ascending(x => x.Status)
                .Descending(x => x.CreatedUtc));

        return inbox.Indexes.CreateOneAsync(deadLettersByAge, cancellationToken: ct);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
