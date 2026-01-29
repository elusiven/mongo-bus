using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;

namespace MongoBus.ClaimCheck;

public sealed class AzureBlobClaimCheckProvider : IClaimCheckProvider
{
    private readonly BlobContainerClient _container;
    private readonly AzureBlobClaimCheckOptions _options;

    public AzureBlobClaimCheckProvider(AzureBlobClaimCheckOptions options)
    {
        _options = options;
        _container = new BlobContainerClient(options.ConnectionString, options.ContainerName);
    }

    public string Name => "azure";

    public async Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: ct);

        var blobName = BuildKey();
        var blob = _container.GetBlobClient(blobName);

        var uploadOptions = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = request.ContentType }
        };

        if (request.Metadata is not null)
            uploadOptions.Metadata = request.Metadata.ToDictionary(k => k.Key, v => v.Value);

        await blob.UploadAsync(request.Data, uploadOptions, ct);

        var length = request.Length ?? (request.Data.CanSeek ? request.Data.Length : 0);
        return new ClaimCheckReference(Name, _options.ContainerName, blobName, length, request.ContentType, request.Metadata);
    }

    public async Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var blob = _container.GetBlobClient(reference.Key);
        var response = await blob.OpenReadAsync(new BlobOpenReadOptions(false), ct);
        return response;
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var blob = _container.GetBlobClient(reference.Key);
        await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: ct);
    }

    public async IAsyncEnumerable<ClaimCheckReference> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var item in _container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, cancellationToken: ct))
        {
            DateTime? createdAt = item.Properties.CreatedOn?.UtcDateTime;
            var metadata = item.Metadata;
            if (metadata != null && metadata.TryGetValue(ClaimCheckConstants.CreatedAtMetadataKey.Replace("-", ""), out var caStr) && DateTime.TryParse(caStr, out var ca))
            {
                // Azure blob metadata keys are alphanumeric and case-insensitive, often stripped of hyphens by some tools,
                // but usually preserved if set via SDK. Let's be careful.
                createdAt = ca;
            }
            else if (metadata != null && metadata.TryGetValue(ClaimCheckConstants.CreatedAtMetadataKey, out var caStr2) && DateTime.TryParse(caStr2, out var ca2))
            {
                createdAt = ca2;
            }

            yield return new ClaimCheckReference(
                Provider: Name,
                Container: _options.ContainerName,
                Key: item.Name,
                Length: item.Properties.ContentLength ?? 0,
                ContentType: item.Properties.ContentType,
                Metadata: metadata?.ToDictionary(k => k.Key, v => v.Value),
                CreatedAt: createdAt);
        }
    }

    private string BuildKey()
    {
        var prefix = string.IsNullOrWhiteSpace(_options.BlobPrefix) ? "" : _options.BlobPrefix!.TrimEnd('/') + "/";
        return $"{prefix}{Guid.NewGuid():N}";
    }
}
