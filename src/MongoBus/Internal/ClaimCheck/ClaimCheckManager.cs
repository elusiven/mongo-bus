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
        var payload = new LimitedReadStream(await OpenPayloadAsync(reference, ct), options.ClaimCheck.Compression.MaxDecompressedBytes);

        if (typeof(Stream).IsAssignableFrom(messageType))
            return payload;

        await using (payload)
        {
            return await serializer.DeserializeAsync(payload, messageType, ct);
        }
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var provider = providerResolver.GetProviderForReference(reference);
        await provider.DeleteAsync(reference, ct);
    }

    private async Task<Stream> OpenPayloadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var storedPayload = await providerResolver.GetProviderForReference(reference).OpenReadAsync(reference, ct);

        if (reference.Metadata is null || !reference.Metadata.TryGetValue(ClaimCheckConstants.CompressionMetadataKey, out var algorithm))
            return storedPayload;

        return await compressorProvider.GetCompressor(algorithm).DecompressAsync(storedPayload, ct);
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

        var serialized = await SerializeToBufferAsync(data!, ct);

        if (serialized.Length < options.ClaimCheck.ThresholdBytes && context.UseClaimCheck != true)
        {
            await serialized.DisposeAsync();
            return null;
        }

        serialized.Position = 0;
        return new StreamInfo(serialized, ClaimCheckConstants.DefaultObjectContentType, serialized.Length, true);
    }

    private async Task<Stream> SerializeToBufferAsync(object data, CancellationToken ct)
    {
        var buffer = new SpillToDiskStream(InMemoryPayloadLimit, Path.GetTempPath());
        try
        {
            await serializer.SerializeAsync(data, buffer, ct);
            await buffer.FlushAsync(ct);
            return buffer;
        }
        catch
        {
            await buffer.DisposeAsync();
            throw;
        }
    }

    // A payload under the threshold is stored inline, and an inline payload over MaxMessageSizeBytes is rejected,
    // so every payload that can be stored inline stays off the disk while a large threshold cannot buffer more in memory.
    private long InMemoryPayloadLimit => Math.Min(options.ClaimCheck.ThresholdBytes, options.MaxMessageSizeBytes);

    private static Dictionary<string, string> CreateMetadata() =>
        new()
        {
            [ClaimCheckConstants.CreatedAtMetadataKey] = DateTime.UtcNow.ToString("O")
        };

    private static ClaimCheckReference EnsureCreatedAt(ClaimCheckReference reference) =>
        reference.CreatedAt == null ? reference with { CreatedAt = DateTime.UtcNow } : reference;

    private sealed record StreamInfo(Stream Stream, string ContentType, long? Length, bool ShouldDispose);
}
