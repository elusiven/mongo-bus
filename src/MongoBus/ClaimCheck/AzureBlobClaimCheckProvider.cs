using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
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

    private string BuildKey()
    {
        var prefix = string.IsNullOrWhiteSpace(_options.BlobPrefix) ? "" : _options.BlobPrefix!.TrimEnd('/') + "/";
        return $"{prefix}{Guid.NewGuid():N}";
    }
}
