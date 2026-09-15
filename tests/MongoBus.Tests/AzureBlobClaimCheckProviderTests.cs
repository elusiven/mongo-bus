using System.Text;
using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class AzureBlobClaimCheckProviderTests(AzuriteFixture azurite) : IClassFixture<AzuriteFixture>
{
    private static readonly DateTime CreatedAt = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    [Fact]
    public async Task PutAsync_WithMongoBusMetadata_ShouldStorePayloadThatOpenReadAsyncReturns()
    {
        var provider = CreateProvider();
        const string payload = "{\"value\":\"large payload\"}";

        var reference = await provider.PutAsync(WriteRequest(payload, MongoBusMetadata()), CancellationToken.None);

        await using var stored = await provider.OpenReadAsync(reference, CancellationToken.None);
        using var reader = new StreamReader(stored);
        (await reader.ReadToEndAsync()).Should().Be(payload);
    }

    [Fact]
    public async Task ListAsync_ShouldReturnMongoBusMetadataUnderItsOriginalKeys()
    {
        var provider = CreateProvider();
        await provider.PutAsync(WriteRequest("payload", MongoBusMetadata()), CancellationToken.None);

        var listed = (await ListReferencesAsync(provider)).Single();

        listed.Metadata.Should().Contain(MongoBusMetadata());
    }

    [Fact]
    public async Task ListAsync_ShouldReportTheCreationTimeFromMetadataAsUtc()
    {
        var provider = CreateProvider();
        await provider.PutAsync(WriteRequest("payload", MongoBusMetadata()), CancellationToken.None);

        var listed = (await ListReferencesAsync(provider)).Single();

        listed.CreatedAt.Should().Be(CreatedAt);
        listed.CreatedAt!.Value.Kind.Should().Be(DateTimeKind.Utc);
    }

    private AzureBlobClaimCheckProvider CreateProvider() =>
        new(new AzureBlobClaimCheckOptions
        {
            ConnectionString = azurite.ConnectionString,
            ContainerName = "claims-" + Guid.NewGuid().ToString("N")
        });

    private static Dictionary<string, string> MongoBusMetadata() => new()
    {
        [ClaimCheckConstants.CreatedAtMetadataKey] = CreatedAt.ToString("O"),
        [ClaimCheckConstants.CompressionMetadataKey] = "gzip"
    };

    private static ClaimCheckWriteRequest WriteRequest(string payload, IReadOnlyDictionary<string, string> metadata) =>
        new(new MemoryStream(Encoding.UTF8.GetBytes(payload)), "application/json", metadata);

    private static async Task<List<ClaimCheckReference>> ListReferencesAsync(IClaimCheckProvider provider)
    {
        var references = new List<ClaimCheckReference>();
        await foreach (var reference in provider.ListAsync(CancellationToken.None))
            references.Add(reference);
        return references;
    }
}
