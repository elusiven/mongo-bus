using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using MongoDB.Driver;

namespace MongoBus.Internal.ClaimCheck;

internal sealed class ClaimCheckCleanupService(
    MongoBusOptions options,
    IEnumerable<IClaimCheckProvider> providers,
    IMongoDatabase db,
    ICloudEventSerializer serializer,
    ILogger<ClaimCheckCleanupService> log) : BackgroundService
{
    private readonly IReadOnlyList<IClaimCheckProvider> _providers = providers.ToList();
    private readonly ClaimCheckReferenceReader _references = new(db, serializer, log);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Not gated on ClaimCheck.Enabled: a message can still request a claim check when it is off.
        if (!options.ClaimCheck.Cleanup.Enabled || _providers.Count == 0)
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

        var expiredPayloads = await ListExpiredPayloadsAsync(ct);
        foreach (var orphanedPayload in await ExcludeReferencedAsync(expiredPayloads, ct))
            await DeleteAsync(orphanedPayload, ct);

        log.LogInformation("Claim-check cleanup finished.");
    }

    private async Task<IReadOnlyList<StoredPayload>> ListExpiredPayloadsAsync(CancellationToken ct)
    {
        var createdBefore = DateTime.UtcNow - options.ClaimCheck.Cleanup.MinimumAge;
        var expiredPayloads = new List<StoredPayload>();

        foreach (var provider in _providers)
        {
            await foreach (var reference in provider.ListAsync(ct))
            {
                if (IsCreatedBefore(reference, createdBefore))
                    expiredPayloads.Add(new StoredPayload(provider, reference));
            }
        }

        return expiredPayloads;
    }

    private static bool IsCreatedBefore(ClaimCheckReference reference, DateTime createdBefore) =>
        reference.CreatedAt is not { } createdAt || createdAt <= createdBefore;

    private async Task<IReadOnlyList<StoredPayload>> ExcludeReferencedAsync(IReadOnlyList<StoredPayload> payloads, CancellationToken ct)
    {
        if (payloads.Count == 0)
            return payloads;

        var unreferencedKeys = payloads.Select(payload => payload.Reference.Key).ToHashSet(StringComparer.Ordinal);
        await foreach (var referencedKey in _references.ReadReferencedKeysAsync(ct))
            unreferencedKeys.Remove(referencedKey);

        return payloads.Where(payload => unreferencedKeys.Contains(payload.Reference.Key)).ToList();
    }

    private async Task DeleteAsync(StoredPayload payload, CancellationToken ct)
    {
        log.LogInformation("Deleting orphaned claim-check: {Provider}/{Key} created at {CreatedAt}",
            payload.Provider.Name, payload.Reference.Key, payload.Reference.CreatedAt);

        await payload.Provider.DeleteAsync(payload.Reference, ct);
    }

    private sealed record StoredPayload(IClaimCheckProvider Provider, ClaimCheckReference Reference);
}
