namespace MongoBus.DependencyInjection;

public sealed class ClaimCheckOptions
{
    public bool Enabled { get; set; }
    public long ThresholdBytes { get; set; } = 256 * 1024;
    public string? ProviderName { get; set; }
    public ClaimCheckCompressionOptions Compression { get; set; } = new();
}

public sealed class ClaimCheckCompressionOptions
{
    public bool Enabled { get; set; }
    public string Algorithm { get; set; } = "gzip";
}

public sealed class AzureBlobClaimCheckOptions
{
    public string ConnectionString { get; set; } = default!;
    public string ContainerName { get; set; } = default!;
    public string? BlobPrefix { get; set; }
}

public sealed class S3ClaimCheckOptions
{
    public string AccessKey { get; set; } = default!;
    public string SecretKey { get; set; } = default!;
    public string ServiceUrl { get; set; } = default!;
    public string BucketName { get; set; } = default!;
    public string? KeyPrefix { get; set; }
    public string? Region { get; set; }
    public bool ForcePathStyle { get; set; } = true;
    public string ProviderName { get; set; } = "s3";
}
