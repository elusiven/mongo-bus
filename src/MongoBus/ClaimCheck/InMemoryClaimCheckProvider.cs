using System.Collections.Concurrent;
using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.ClaimCheck;

public sealed class InMemoryClaimCheckProvider : IClaimCheckProvider
{
    private readonly ConcurrentDictionary<string, byte[]> _store = new();

    public string Name => "memory";

    public async Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct)
    {
        await using var ms = new MemoryStream();
        await request.Data.CopyToAsync(ms, ct);
        var bytes = ms.ToArray();

        var key = Guid.NewGuid().ToString("N");
        _store[key] = bytes;

        return new ClaimCheckReference(Name, "memory", key, bytes.LongLength, request.ContentType, request.Metadata);
    }

    public Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        if (!_store.TryGetValue(reference.Key, out var bytes))
            throw new InvalidOperationException("Missing claim-check payload.");

        Stream stream = new MemoryStream(bytes, writable: false);
        return Task.FromResult(stream);
    }
}
