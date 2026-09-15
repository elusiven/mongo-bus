using Amazon.S3;
using Testcontainers.Minio;
using Xunit;

namespace MongoBus.Tests;

public class MinioFixture : IAsyncLifetime
{
    // Deliberately an older release without AWS flexible-checksum support, standing in for
    // S3-compatible stores that reject the checksums AWS SDK v4 sends by default.
    private readonly MinioContainer _container = new MinioBuilder("minio/minio:RELEASE.2023-01-31T02-24-19Z")
        .Build();

    public string ServiceUrl => _container.GetConnectionString();
    public string AccessKey => _container.GetAccessKey();
    public string SecretKey => _container.GetSecretKey();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.StopAsync();
    }

    public async Task CreateBucketAsync(string bucketName)
    {
        using var client = CreateClient();
        await client.PutBucketAsync(bucketName);
    }

    public async Task<string> GetObjectMetadataValueAsync(string bucketName, string key, string metadataName)
    {
        using var client = CreateClient();
        var response = await client.GetObjectMetadataAsync(bucketName, key);
        return response.Metadata[metadataName];
    }

    private AmazonS3Client CreateClient() =>
        new(AccessKey, SecretKey, new AmazonS3Config
        {
            ServiceURL = ServiceUrl,
            ForcePathStyle = true
        });
}
