using MongoBus.Models;

namespace MongoBus.Abstractions;

public sealed record ClaimCheckWriteRequest(
    Stream Data,
    string? ContentType = null,
    IReadOnlyDictionary<string, string>? Metadata = null,
    long? Length = null);

public interface IClaimCheckProvider
{
    string Name { get; }
    Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct);
    Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct);
    Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct);
    IAsyncEnumerable<ClaimCheckReference> ListAsync(CancellationToken ct);
}
