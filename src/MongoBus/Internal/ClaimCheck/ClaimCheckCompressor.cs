using System.IO.Compression;

namespace MongoBus.Internal.ClaimCheck;

internal interface IClaimCheckCompressor
{
    string Algorithm { get; }
    Task<Stream> CompressAsync(Stream source, CancellationToken ct);
    Task<Stream> DecompressAsync(Stream source, CancellationToken ct);
}

internal sealed class GZipClaimCheckCompressor : IClaimCheckCompressor
{
    public string Algorithm => "gzip";

    public async Task<Stream> CompressAsync(Stream source, CancellationToken ct)
    {
        var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            await source.CopyToAsync(gzip, ct);
        }
        output.Position = 0;
        return output;
    }

    public Task<Stream> DecompressAsync(Stream source, CancellationToken ct)
    {
        return Task.FromResult<Stream>(new GZipStream(source, CompressionMode.Decompress));
    }
}

internal interface IClaimCheckCompressorProvider
{
    IClaimCheckCompressor GetCompressor(string algorithm);
}

internal sealed class ClaimCheckCompressorProvider(IEnumerable<IClaimCheckCompressor> compressors) : IClaimCheckCompressorProvider
{
    private readonly Dictionary<string, IClaimCheckCompressor> _compressors = compressors
        .ToDictionary(x => x.Algorithm, x => x, StringComparer.OrdinalIgnoreCase);

    public IClaimCheckCompressor GetCompressor(string algorithm)
    {
        if (_compressors.TryGetValue(algorithm, out var compressor))
            return compressor;

        throw new NotSupportedException($"Compression algorithm '{algorithm}' is not supported.");
    }
}
