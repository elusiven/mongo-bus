using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Models;

namespace MongoBus.Internal.ClaimCheck;

public interface IClaimCheckManager
{
    Task<ClaimCheckDecision> TryStoreAsync<T>(PublishContext<T> context, CancellationToken ct);
    Task<object> ResolveAsync(ClaimCheckReference reference, Type messageType, CancellationToken ct);
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
        var data = context.Data;
        var claimCheckEnabled = options.ClaimCheck.Enabled;

        // If globally enabled, we ALWAYS check if it exceeds threshold.
        // If globally disabled, we only claim-check if explicitly requested per message.
        if (!claimCheckEnabled && context.UseClaimCheck != true)
            return new ClaimCheckDecision(false, null);

        if (data is null)
            return new ClaimCheckDecision(false, null);

        var provider = providerResolver.GetProvider(options.ClaimCheck.ProviderName);

        Stream streamData;
        string contentType;
        long? length = null;
        bool shouldDisposeStream = false;

        if (data is Stream s)
        {
            streamData = s;
            contentType = ClaimCheckConstants.DefaultStreamContentType;
            length = s.CanSeek ? s.Length : null;
        }
        else
        {
            var temp = await TempFile.CreateAsync(ct);
            shouldDisposeStream = true;
            await serializer.SerializeAsync(data, temp.Stream, ct);
            await temp.Stream.FlushAsync(ct);
            
            if (temp.Stream.Length < options.ClaimCheck.ThresholdBytes)
            {
                // If it's below threshold, we only claim-check if explicitly forced
                if (context.UseClaimCheck != true)
                {
                    await temp.DisposeAsync();
                    return new ClaimCheckDecision(false, null);
                }
            }
            
            temp.Stream.Position = 0;
            streamData = temp.Stream;
            contentType = ClaimCheckConstants.DefaultObjectContentType;
            length = temp.Stream.Length;
        }

        try
        {
            var metadata = new Dictionary<string, string>();
            if (options.ClaimCheck.Compression.Enabled)
            {
                var compressor = compressorProvider.GetCompressor(options.ClaimCheck.Compression.Algorithm);
                var compressedStream = await compressor.CompressAsync(streamData, ct);
                
                if (shouldDisposeStream) await streamData.DisposeAsync();
                
                streamData = compressedStream;
                shouldDisposeStream = true;
                length = streamData.Length;
                metadata["x-mongobus-compression"] = compressor.Algorithm;
            }

            var claimReference = await provider.PutAsync(
                new ClaimCheckWriteRequest(streamData, contentType, metadata, length), ct);
                
            return new ClaimCheckDecision(true, claimReference);
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

        if (reference.Metadata != null && reference.Metadata.TryGetValue("x-mongobus-compression", out var algorithm))
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

    private sealed class TempFile : IAsyncDisposable
    {
        private readonly string _path;
        public FileStream Stream { get; }

        private TempFile(string path, FileStream stream)
        {
            _path = path;
            Stream = stream;
        }

        public static Task<TempFile> CreateAsync(CancellationToken ct)
        {
            var path = Path.Combine(Path.GetTempPath(), $"mongobus-claimcheck-{Guid.NewGuid():N}.json");
            var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
            return Task.FromResult(new TempFile(path, stream));
        }

        public ValueTask DisposeAsync()
        {
            Stream.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
