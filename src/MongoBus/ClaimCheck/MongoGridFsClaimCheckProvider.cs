using MongoBus.Abstractions;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace MongoBus.ClaimCheck;

public sealed class MongoGridFsClaimCheckProvider(IMongoDatabase database, string bucketName = "claimcheck") : IClaimCheckProvider
{
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

        await _bucket.UploadFromStreamAsync(key, request.Data, options, ct);

        long length = request.Length ?? 0;

        return new ClaimCheckReference(
            Provider: Name,
            Container: bucketName,
            Key: key,
            Length: length,
            ContentType: request.ContentType,
            Metadata: request.Metadata);
    }

    public async Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        return await _bucket.OpenDownloadStreamByNameAsync(reference.Key, cancellationToken: ct);
    }
}
