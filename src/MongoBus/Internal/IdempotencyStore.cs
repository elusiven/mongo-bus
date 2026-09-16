using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

/// <summary>
/// Decides which consumer may handle a CloudEvent. The claim is made by inserting it, so the winner is settled by
/// the database rather than by reading first and acting afterwards, which let two consumers both find nothing and
/// both handle the same event.
/// </summary>
internal sealed class IdempotencyStore
{
    private readonly IMongoCollection<InboxDedupRecord> _claims;

    public IdempotencyStore(IMongoDatabase db) =>
        _claims = db.GetCollection<InboxDedupRecord>(MongoBusConstants.InboxDedupCollectionName);

    /// <returns>True when this caller took the claim, false when another consumer already holds it.</returns>
    public async Task<bool> TryClaimAsync(string endpointId, string cloudEventId, CancellationToken ct)
    {
        var claim = new InboxDedupRecord
        {
            Id = ClaimIdFor(endpointId, cloudEventId),
            CreatedUtc = DateTime.UtcNow
        };

        try
        {
            await _claims.InsertOneAsync(claim, cancellationToken: ct);
            return true;
        }
        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            return false;
        }
    }

    public Task ReleaseAsync(string endpointId, string cloudEventId, CancellationToken ct) =>
        _claims.DeleteOneAsync(x => x.Id == ClaimIdFor(endpointId, cloudEventId), ct);

    private static string ClaimIdFor(string endpointId, string cloudEventId) => $"{endpointId}:{cloudEventId}";
}
