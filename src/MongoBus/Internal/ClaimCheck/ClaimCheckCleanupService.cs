using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal.ClaimCheck;

internal sealed class ClaimCheckCleanupService(
    MongoBusOptions options,
    IEnumerable<IClaimCheckProvider> providers,
    IMongoDatabase db,
    ILogger<ClaimCheckCleanupService> log) : BackgroundService
{
    private readonly IMongoCollection<InboxMessage> _inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.ClaimCheck.Enabled || !options.ClaimCheck.Cleanup.Enabled)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCleanupAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error during claim-check cleanup");
            }

            await Task.Delay(options.ClaimCheck.Cleanup.Interval, stoppingToken);
        }
    }

    private async Task RunCleanupAsync(CancellationToken ct)
    {
        log.LogInformation("Starting claim-check cleanup...");

        var now = DateTime.UtcNow;
        var minAge = options.ClaimCheck.Cleanup.MinimumAge;
        var threshold = now - minAge;

        foreach (var provider in providers)
        {
            if (ct.IsCancellationRequested) break;

            log.LogDebug("Cleaning up provider: {ProviderName}", provider.Name);

            await foreach (var reference in provider.ListAsync(ct))
            {
                if (reference.CreatedAt.HasValue && reference.CreatedAt.Value > threshold)
                {
                    // Too young to die
                    continue;
                }

                // Double check if it's still referenced in Mongo.
                // This is a safety measure. If fan-out exists, at least one endpoint might still need it.
                // However, we look for ANY reference to this specific KEY in the entire inbox.
                
                // Optimization: if there are many references, this might be slow.
                // But this service runs in background and only for OLD items.
                
                var isReferenced = await _inbox.Find(x => x.PayloadJson.Contains(reference.Key)).AnyAsync(ct);
                if (!isReferenced)
                {
                    log.LogInformation("Deleting orphaned claim-check: {Provider}/{Key} created at {CreatedAt}", 
                        provider.Name, reference.Key, reference.CreatedAt);
                    
                    await provider.DeleteAsync(reference, ct);
                }
            }
        }

        log.LogInformation("Claim-check cleanup finished.");
    }
}
