using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.Internal.ClaimCheck;

internal interface IClaimCheckProviderResolver
{
    IClaimCheckProvider GetProvider(string? name);
    IClaimCheckProvider GetProviderForReference(ClaimCheckReference reference);
}

internal sealed class ClaimCheckProviderResolver(IEnumerable<IClaimCheckProvider> providers) : IClaimCheckProviderResolver
{
    private readonly IReadOnlyDictionary<string, IClaimCheckProvider> _providers = providers
        .GroupBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
        .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

    public IClaimCheckProvider GetProvider(string? name)
    {
        if (!string.IsNullOrWhiteSpace(name))
        {
            if (_providers.TryGetValue(name, out var provider))
                return provider;

            throw new InvalidOperationException($"Claim check provider '{name}' is not registered.");
        }

        if (_providers.Count == 1)
            return _providers.Values.First();

        throw new InvalidOperationException("Claim check provider name is not set and multiple providers are registered.");
    }

    public IClaimCheckProvider GetProviderForReference(ClaimCheckReference reference)
    {
        if (_providers.TryGetValue(reference.Provider, out var provider))
            return provider;

        throw new InvalidOperationException($"Claim check provider '{reference.Provider}' is not registered.");
    }
}
