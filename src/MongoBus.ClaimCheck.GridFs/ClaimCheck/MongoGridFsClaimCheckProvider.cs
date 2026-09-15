using MongoBus.Abstractions;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace MongoBus.ClaimCheck;

public sealed class MongoGridFsClaimCheckProvider(IMongoDatabase database, string bucketName = "claimcheck") : IClaimCheckProvider
{
    // The bucket may hold files other applications wrote; MongoBus marks every payload it stores with its creation time.
    private static readonly FilterDefinition<GridFSFileInfo> StoredByMongoBus =
        Builders<GridFSFileInfo>.Filter.Exists($"metadata.{ClaimCheckConstants.CreatedAtMetadataKey}");

    private readonly IGridFSBucket _bucket = new GridFSBucket(database, new GridFSBucketOptions
    {
        BucketName = bucketName
    });

    public string Name => "gridfs";

    public async Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct)
    {
        var key = Guid.NewGuid().ToString("N");
        var metadata = new BsonDocument();
        if (request.Metadata != null)
        {
            foreach (var kvp in request.Metadata)
            {
                metadata[kvp.Key] = kvp.Value;
            }
        }

        if (!string.IsNullOrEmpty(request.ContentType))
        {
            metadata["contentType"] = request.ContentType;
        }

        var options = new GridFSUploadOptions
        {
            Metadata = metadata
        };

        var fileId = await _bucket.UploadFromStreamAsync(key, request.Data, options, ct);
        var length = request.Length ?? await StoredLengthAsync(fileId, ct);

        return new ClaimCheckReference(
            Provider: Name,
            Container: bucketName,
            Key: key,
            Length: length,
            ContentType: request.ContentType,
            Metadata: request.Metadata);
    }

    private async Task<long> StoredLengthAsync(ObjectId fileId, CancellationToken ct)
    {
        var storedFile = await _bucket.Find(Builders<GridFSFileInfo>.Filter.Eq(x => x.Id, fileId)).FirstAsync(ct);
        return storedFile.Length;
    }

    public async Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        return await _bucket.OpenDownloadStreamByNameAsync(reference.Key, cancellationToken: ct);
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, reference.Key);
        var fileInfo = await _bucket.Find(filter).FirstOrDefaultAsync(ct);
        if (fileInfo != null)
        {
            await _bucket.DeleteAsync(fileInfo.Id, ct);
        }
    }

    public async IAsyncEnumerable<ClaimCheckReference> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using var cursor = await _bucket.FindAsync(StoredByMongoBus, cancellationToken: ct);
        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var file in cursor.Current)
            {
                var metadata = new Dictionary<string, string>();
                if (file.Metadata != null)
                {
                    foreach (var element in file.Metadata)
                    {
                        metadata[element.Name] = element.Value.ToString() ?? "";
                    }
                }

                DateTime? createdAt = file.UploadDateTime;
                if (metadata.TryGetValue(ClaimCheckConstants.CreatedAtMetadataKey, out var caStr) && DateTime.TryParse(caStr, out var ca))
                {
                    createdAt = ca;
                }

                yield return new ClaimCheckReference(
                    Provider: Name,
                    Container: bucketName,
                    Key: file.Filename,
                    Length: file.Length,
                    ContentType: metadata.GetValueOrDefault("contentType"),
                    Metadata: metadata,
                    CreatedAt: createdAt);
            }
        }
    }
}
