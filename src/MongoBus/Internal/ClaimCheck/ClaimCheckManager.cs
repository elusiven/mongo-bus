using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Models;

namespace MongoBus.Internal.ClaimCheck;

public interface IClaimCheckManager
{
    Task<ClaimCheckDecision> TryStoreAsync<T>(PublishContext<T> context, CancellationToken ct);
    Task<object> ResolveAsync(ClaimCheckReference reference, Type messageType, CancellationToken ct);
    Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct);
}

public sealed record ClaimCheckDecision(bool IsClaimCheck, ClaimCheckReference? Reference);

internal sealed class ClaimCheckManager(
    MongoBusOptions options,
    IClaimCheckProviderResolver providerResolver,
    IClaimCheckDataSerializer serializer,
    IClaimCheckCompressorProvider compressorProvider) : IClaimCheckManager
{
    public async Task<ClaimCheckDecision> TryStoreAsync<T>(PublishContext<T> context, CancellationToken ct)
    {
        if (!ShouldAttemptClaimCheck(context))
            return new ClaimCheckDecision(false, null);

        var data = context.Data;
        if (data is null)
            return new ClaimCheckDecision(false, null);

        var provider = providerResolver.GetProvider(options.ClaimCheck.ProviderName);
        var streamInfo = await CreateStreamAsync(context, data, ct);
        if (streamInfo is null)
            return new ClaimCheckDecision(false, null);

        var streamData = streamInfo.Stream;
        var length = streamInfo.Length;
        var shouldDisposeStream = streamInfo.ShouldDispose;

        try
        {
            var metadata = CreateMetadata();

            if (options.ClaimCheck.Compression.Enabled)
            {
                var compressor = compressorProvider.GetCompressor(options.ClaimCheck.Compression.Algorithm);
                var compressedStream = await compressor.CompressAsync(streamData, ct);

                if (shouldDisposeStream) await streamData.DisposeAsync();

                streamData = compressedStream;
                shouldDisposeStream = true;
                length = streamData.Length;
                metadata[ClaimCheckConstants.CompressionMetadataKey] = compressor.Algorithm;
            }

            var claimReference = await provider.PutAsync(
                new ClaimCheckWriteRequest(streamData, streamInfo.ContentType, metadata, length), ct);

            return new ClaimCheckDecision(true, EnsureCreatedAt(claimReference));
        }
        finally
        {
            if (shouldDisposeStream) await streamData.DisposeAsync();
        }
    }

    public async Task<object> ResolveAsync(ClaimCheckReference reference, Type messageType, CancellationToken ct)
    {
        var provider = providerResolver.GetProviderForReference(reference);
        var stream = await provider.OpenReadAsync(reference, ct);

        if (reference.Metadata != null && reference.Metadata.TryGetValue(ClaimCheckConstants.CompressionMetadataKey, out var algorithm))
        {
            var compressor = compressorProvider.GetCompressor(algorithm);
            stream = await compressor.DecompressAsync(stream, ct);
        }

        if (typeof(Stream).IsAssignableFrom(messageType))
            return stream;

        await using (stream)
        {
            return await serializer.DeserializeAsync(stream, messageType, ct);
        }
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var provider = providerResolver.GetProviderForReference(reference);
        await provider.DeleteAsync(reference, ct);
    }

    private bool ShouldAttemptClaimCheck<T>(PublishContext<T> context)
    {
        // If globally enabled, we ALWAYS check if it exceeds threshold.
        // If globally disabled, we only claim-check if explicitly requested per message.
        return options.ClaimCheck.Enabled || context.UseClaimCheck == true;
    }

    private async Task<StreamInfo?> CreateStreamAsync<T>(PublishContext<T> context, T data, CancellationToken ct)
    {
        if (data is Stream stream)
        {
            var streamLength = stream.CanSeek ? stream.Length : (long?)null;
            return new StreamInfo(stream, ClaimCheckConstants.DefaultStreamContentType, streamLength, false);
        }

        var temp = await TempFile.CreateAsync();
        await serializer.SerializeAsync(data!, temp.Stream, ct);
        await temp.Stream.FlushAsync(ct);

        if (temp.Stream.Length < options.ClaimCheck.ThresholdBytes && context.UseClaimCheck != true)
        {
            await temp.DisposeAsync();
            return null;
        }

        temp.Stream.Position = 0;
        return new StreamInfo(temp.Stream, ClaimCheckConstants.DefaultObjectContentType, temp.Stream.Length, true);
    }

    private static Dictionary<string, string> CreateMetadata() =>
        new()
        {
            [ClaimCheckConstants.CreatedAtMetadataKey] = DateTime.UtcNow.ToString("O")
        };

    private static ClaimCheckReference EnsureCreatedAt(ClaimCheckReference reference) =>
        reference.CreatedAt == null ? reference with { CreatedAt = DateTime.UtcNow } : reference;

    private sealed record StreamInfo(Stream Stream, string ContentType, long? Length, bool ShouldDispose);

    private sealed class TempFile : IAsyncDisposable
    {
        public FileStream Stream { get; }

        private TempFile(FileStream stream)
        {
            Stream = stream;
        }

        public static Task<TempFile> CreateAsync()
        {
            var path = Path.Combine(Path.GetTempPath(), $"mongobus-claimcheck-{Guid.NewGuid():N}.json");
            var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
            return Task.FromResult(new TempFile(stream));
        }

        public ValueTask DisposeAsync()
        {
            Stream.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
