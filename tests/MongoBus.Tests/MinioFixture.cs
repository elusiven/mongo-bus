using Amazon.S3;
using Testcontainers.Minio;
using Xunit;

namespace MongoBus.Tests;

public class MinioFixture : IAsyncLifetime
{
    private readonly MinioContainer _container = new MinioBuilder("minio/minio:RELEASE.2023-01-31T02-24-19Z")
        .Build();

    public string ServiceUrl => _container.GetConnectionString();
    public string AccessKey => _container.GetAccessKey();
    public string SecretKey => _container.GetSecretKey();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.StopAsync();
    }

    public async Task CreateBucketAsync(string bucketName)
    {
        using var client = new AmazonS3Client(AccessKey, SecretKey, new AmazonS3Config
        {
            ServiceURL = ServiceUrl,
            ForcePathStyle = true
        });

        await client.PutBucketAsync(bucketName);
    }
}
