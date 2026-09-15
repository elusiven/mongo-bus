using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Internal;

/// <summary>
/// Keeps the TTL indexes that expire handled messages in line with configuration, and removes
/// the TTL indexes earlier versions created. Those expired documents by age alone, so pending,
/// delayed and dead-lettered messages (and claim-check payloads they referenced) were deleted too.
/// </summary>
internal static class RetentionIndexes
{
    private const int IndexOptionsConflict = 85;
    private const string LegacyMessageTtlField = "CreatedUtc";
    private const string LegacyGridFsTtlField = "uploadDate";
    private const string GridFsFilesCollectionSuffix = ".files";
    private static readonly TimeSpan LegacyGridFsTtlMargin = TimeSpan.FromDays(1);

    public static async Task EnsureAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        string field,
        TimeSpan expireAfter,
        CancellationToken ct)
    {
        var index = new CreateIndexModel<TDocument>(
            Builders<TDocument>.IndexKeys.Ascending(field),
            new CreateIndexOptions { ExpireAfter = expireAfter });

        try
        {
            await collection.Indexes.CreateOneAsync(index, cancellationToken: ct);
        }
        catch (MongoCommandException ex) when (ex.Code == IndexOptionsConflict)
        {
            await UpdateExpiryAsync(collection, field, expireAfter, ct);
        }
    }

    public static Task DropLegacyMessageTtlIndexAsync<TDocument>(IMongoCollection<TDocument> collection, CancellationToken ct) =>
        DropTtlIndexesAsync(collection, index => HasSingleKey(index, LegacyMessageTtlField), ct);

    /// <summary>
    /// Earlier versions put a TTL index on <c>uploadDate</c> of every GridFS bucket in the database,
    /// expiring after <paramref name="messageTtl"/> plus one day. Only indexes with exactly that
    /// signature are dropped, so TTL indexes users created themselves are left alone.
    /// </summary>
    public static async Task DropLegacyGridFsTtlIndexesAsync(IMongoDatabase db, TimeSpan messageTtl, CancellationToken ct)
    {
        var legacyExpireAfterSeconds = (long)messageTtl.Add(LegacyGridFsTtlMargin).TotalSeconds;

        using var cursor = await db.ListCollectionNamesAsync(cancellationToken: ct);
        var collectionNames = await cursor.ToListAsync(ct);

        foreach (var filesCollectionName in collectionNames.Where(name => name.EndsWith(GridFsFilesCollectionSuffix, StringComparison.Ordinal)))
        {
            var filesCollection = db.GetCollection<BsonDocument>(filesCollectionName);
            await DropTtlIndexesAsync(
                filesCollection,
                index => HasSingleKey(index, LegacyGridFsTtlField) && ExpireAfterSeconds(index) == legacyExpireAfterSeconds,
                ct);
        }
    }

    private static async Task UpdateExpiryAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        string field,
        TimeSpan expireAfter,
        CancellationToken ct)
    {
        var command = new BsonDocument
        {
            ["collMod"] = collection.CollectionNamespace.CollectionName,
            ["index"] = new BsonDocument
            {
                ["keyPattern"] = new BsonDocument(field, 1),
                ["expireAfterSeconds"] = (long)expireAfter.TotalSeconds
            }
        };

        await collection.Database.RunCommandAsync<BsonDocument>(command, cancellationToken: ct);
    }

    private static async Task DropTtlIndexesAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        Func<BsonDocument, bool> isObsolete,
        CancellationToken ct)
    {
        using var cursor = await collection.Indexes.ListAsync(ct);
        var indexes = await cursor.ToListAsync(ct);

        foreach (var index in indexes.Where(index => index.Contains("expireAfterSeconds") && isObsolete(index)))
            await collection.Indexes.DropOneAsync(index["name"].AsString, ct);
    }

    private static bool HasSingleKey(BsonDocument index, string field)
    {
        var key = index["key"].AsBsonDocument;
        return key.ElementCount == 1 && key.Contains(field);
    }

    private static long ExpireAfterSeconds(BsonDocument index) => index["expireAfterSeconds"].ToInt64();
}
