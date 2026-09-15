using System.Text;
using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class S3ClaimCheckProviderTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    private const int S3ListPageSize = 1000;

    [Fact]
    public async Task PutAsync_ShouldStorePayloadThatOpenReadAsyncReturns()
    {
        var provider = await CreateProviderWithEmptyBucketAsync();
        const string payload = "{\"value\":\"large payload\"}";

        var reference = await provider.PutAsync(WriteRequest(payload, "application/json"), CancellationToken.None);

        await using var stored = await provider.OpenReadAsync(reference, CancellationToken.None);
        using var reader = new StreamReader(stored);
        (await reader.ReadToEndAsync()).Should().Be(payload);
        reference.Provider.Should().Be("s3");
        reference.Length.Should().Be(Encoding.UTF8.GetByteCount(payload));
        reference.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task PutAsync_WithServiceUrlAndRegion_ShouldUseTheServiceUrl()
    {
        var provider = await CreateProviderWithEmptyBucketAsync(options => options.Region = "us-east-1");

        var reference = await provider.PutAsync(WriteRequest("payload"), CancellationToken.None);

        (await ListReferencesAsync(provider)).Select(x => x.Key).Should().Equal(reference.Key);
    }

    [Fact]
    public async Task PutAsync_WithKeyPrefix_ShouldStoreObjectUnderPrefix()
    {
        var provider = await CreateProviderWithEmptyBucketAsync(options => options.KeyPrefix = "claims/");

        var reference = await provider.PutAsync(WriteRequest("payload"), CancellationToken.None);

        reference.Key.Should().StartWith("claims/");
        (await ListReferencesAsync(provider)).Select(x => x.Key).Should().Equal(reference.Key);
    }

    [Fact]
    public async Task DeleteAsync_ShouldRemoveOnlyTheReferencedObject()
    {
        var provider = await CreateProviderWithEmptyBucketAsync();
        var kept = await provider.PutAsync(WriteRequest("kept"), CancellationToken.None);
        var deleted = await provider.PutAsync(WriteRequest("deleted"), CancellationToken.None);

        await provider.DeleteAsync(deleted, CancellationToken.None);

        (await ListReferencesAsync(provider)).Select(x => x.Key).Should().Equal(kept.Key);
    }

    [Fact]
    public async Task ListAsync_OnEmptyBucket_ShouldReturnNoReferences()
    {
        var provider = await CreateProviderWithEmptyBucketAsync();

        var listed = await ListReferencesAsync(provider);

        listed.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_ShouldReportLengthAndCreationTimeOfEachObject()
    {
        var provider = await CreateProviderWithEmptyBucketAsync();
        var stored = await provider.PutAsync(WriteRequest("12345"), CancellationToken.None);

        var listed = (await ListReferencesAsync(provider)).Single();

        listed.Key.Should().Be(stored.Key);
        listed.Container.Should().Be(stored.Container);
        listed.Length.Should().Be(5);
        listed.CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task ListAsync_ShouldReturnEveryObject_WhenListingSpansMultiplePages()
    {
        var provider = await CreateProviderWithEmptyBucketAsync();
        var objectCount = S3ListPageSize + 1;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, objectCount),
            new ParallelOptions { MaxDegreeOfParallelism = 16 },
            async (index, ct) => await provider.PutAsync(WriteRequest($"payload-{index}"), ct));

        var listed = await ListReferencesAsync(provider);

        listed.Should().HaveCount(objectCount);
    }

    private async Task<S3ClaimCheckProvider> CreateProviderWithEmptyBucketAsync(Action<S3ClaimCheckOptions>? customize = null)
    {
        var bucketName = "claim-checks-" + Guid.NewGuid().ToString("N");
        await minio.CreateBucketAsync(bucketName);

        var options = new S3ClaimCheckOptions
        {
            ServiceUrl = minio.ServiceUrl,
            AccessKey = minio.AccessKey,
            SecretKey = minio.SecretKey,
            BucketName = bucketName
        };
        customize?.Invoke(options);

        return new S3ClaimCheckProvider(options);
    }

    private static ClaimCheckWriteRequest WriteRequest(string payload, string? contentType = null) =>
        new(new MemoryStream(Encoding.UTF8.GetBytes(payload)), contentType);

    private static async Task<List<ClaimCheckReference>> ListReferencesAsync(IClaimCheckProvider provider)
    {
        var references = new List<ClaimCheckReference>();
        await foreach (var reference in provider.ListAsync(CancellationToken.None))
            references.Add(reference);
        return references;
    }
}
