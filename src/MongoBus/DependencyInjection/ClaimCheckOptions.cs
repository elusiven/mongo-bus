namespace MongoBus.DependencyInjection;

public sealed class ClaimCheckOptions
{
    public bool Enabled { get; set; }
    public long ThresholdBytes { get; set; } = 256 * 1024;
    public string? ProviderName { get; set; }
    public ClaimCheckCompressionOptions Compression { get; set; } = new();
    public ClaimCheckCleanupOptions Cleanup { get; set; } = new();
}

public sealed class ClaimCheckCleanupOptions
{
    public bool Enabled { get; set; } = true;
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(6);
    public TimeSpan MinimumAge { get; set; } = TimeSpan.FromDays(8);
}

public sealed class ClaimCheckCompressionOptions
{
    public bool Enabled { get; set; }
    public string Algorithm { get; set; } = "gzip";

    /// <summary>
    /// Maximum number of bytes a compressed claim-check payload may expand to when
    /// decompressed. Guards against decompression bombs from untrusted publishers.
    /// Defaults to 100 MiB.
    /// </summary>
    public long MaxDecompressedBytes { get; set; } = 100L * 1024 * 1024;
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
