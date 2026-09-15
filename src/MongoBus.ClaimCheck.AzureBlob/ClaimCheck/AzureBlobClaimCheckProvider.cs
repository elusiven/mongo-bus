using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;

namespace MongoBus.ClaimCheck;

public sealed class AzureBlobClaimCheckProvider : IClaimCheckProvider
{
    // Azure only accepts metadata names that are valid C# identifiers, so the hyphenated MongoBus
    // metadata keys are stored with underscores and translated back when blobs are listed.
    private static readonly IReadOnlyDictionary<string, string> MetadataKeysByBlobMetadataName =
        new[] { ClaimCheckConstants.CreatedAtMetadataKey, ClaimCheckConstants.CompressionMetadataKey }
            .ToDictionary(ToBlobMetadataName, key => key, StringComparer.OrdinalIgnoreCase);

    private readonly BlobContainerClient _container;
    private readonly AzureBlobClaimCheckOptions _options;
    private readonly string? _blobPrefix;

    public AzureBlobClaimCheckProvider(AzureBlobClaimCheckOptions options)
    {
        _options = options;
        _container = new BlobContainerClient(options.ConnectionString, options.ContainerName);
        _blobPrefix = string.IsNullOrWhiteSpace(options.BlobPrefix) ? null : options.BlobPrefix.TrimEnd('/') + "/";
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
            uploadOptions.Metadata = request.Metadata.ToDictionary(entry => ToBlobMetadataName(entry.Key), entry => entry.Value);

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
        await foreach (var item in _container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix: _blobPrefix, cancellationToken: ct))
        {
            var metadata = item.Metadata?.ToDictionary(entry => FromBlobMetadataName(entry.Key), entry => entry.Value);
            if (!IsStoredByMongoBus(metadata))
                continue;

            yield return new ClaimCheckReference(
                Provider: Name,
                Container: _options.ContainerName,
                Key: item.Name,
                Length: item.Properties.ContentLength ?? 0,
                ContentType: item.Properties.ContentType,
                Metadata: metadata,
                CreatedAt: CreatedAtFrom(metadata) ?? item.Properties.CreatedOn?.UtcDateTime);
        }
    }

    private static string ToBlobMetadataName(string key) => key.Replace('-', '_');

    private static string FromBlobMetadataName(string name) => MetadataKeysByBlobMetadataName.GetValueOrDefault(name, name);

    // The container may hold blobs other applications wrote; MongoBus marks every payload it stores with its creation time.
    private static bool IsStoredByMongoBus([NotNullWhen(true)] IReadOnlyDictionary<string, string>? metadata) =>
        metadata is not null && metadata.ContainsKey(ClaimCheckConstants.CreatedAtMetadataKey);

    private static DateTime? CreatedAtFrom(IReadOnlyDictionary<string, string> metadata) =>
        DateTime.TryParse(metadata[ClaimCheckConstants.CreatedAtMetadataKey], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var createdAt)
            ? createdAt
            : null;

    private string BuildKey() => $"{_blobPrefix}{Guid.NewGuid():N}";
}
