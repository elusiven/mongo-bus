using System.Text;
using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class GridFsClaimCheckProviderTests(MongoDbFixture fixture)
{
    private const string BucketName = "claimcheck";

    [Fact]
    public async Task ListAsync_ShouldSkipFilesWithoutMongoBusMetadata()
    {
        var database = CreateDatabase();
        var provider = new MongoGridFsClaimCheckProvider(database, BucketName);
        var stored = await provider.PutAsync(WriteRequest("payload"), CancellationToken.None);
        await new GridFSBucket(database, new GridFSBucketOptions { BucketName = BucketName })
            .UploadFromBytesAsync("reports/2026-09.csv", Encoding.UTF8.GetBytes("month,total"));

        var listed = await ListReferencesAsync(provider);

        listed.Select(x => x.Key).Should().Equal(stored.Key);
    }

    [Fact]
    public async Task PutAsync_WithoutAKnownLength_ShouldReportTheNumberOfBytesStored()
    {
        const string payload = "{\"value\":\"stored without a declared length\"}";
        var provider = new MongoGridFsClaimCheckProvider(CreateDatabase(), BucketName);

        var stored = await provider.PutAsync(WriteRequest(payload), CancellationToken.None);

        stored.Length.Should().Be(Encoding.UTF8.GetByteCount(payload));
    }

    private IMongoDatabase CreateDatabase() =>
        new MongoClient(fixture.ConnectionString).GetDatabase("gridfs_provider_" + Guid.NewGuid().ToString("N"));

    private static ClaimCheckWriteRequest WriteRequest(string payload) =>
        new(
            new MemoryStream(Encoding.UTF8.GetBytes(payload)),
            "application/json",
            new Dictionary<string, string> { [ClaimCheckConstants.CreatedAtMetadataKey] = DateTime.UtcNow.ToString("O") });

    private static async Task<List<ClaimCheckReference>> ListReferencesAsync(IClaimCheckProvider provider)
    {
        var references = new List<ClaimCheckReference>();
        await foreach (var reference in provider.ListAsync(CancellationToken.None))
            references.Add(reference);
        return references;
    }
}
