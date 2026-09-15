using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using MongoBus.DependencyInjection;

namespace MongoBus.ClaimCheck;

internal static class S3ClientConfiguration
{
    private static readonly string[] AmazonS3HostSuffixes = [".amazonaws.com", ".amazonaws.com.cn"];

    public static AmazonS3Config Create(S3ClaimCheckOptions options) =>
        IsS3CompatibleStore(options.ServiceUrl)
            ? ForS3CompatibleStore(options)
            : ForAmazonS3(options);

    private static bool IsS3CompatibleStore(string? serviceUrl) =>
        !string.IsNullOrWhiteSpace(serviceUrl) && !IsAmazonS3Host(serviceUrl);

    private static bool IsAmazonS3Host(string serviceUrl) =>
        Uri.TryCreate(serviceUrl, UriKind.Absolute, out var uri)
        && AmazonS3HostSuffixes.Any(suffix => ("." + uri.Host).EndsWith(suffix, StringComparison.OrdinalIgnoreCase));

    // Amazon S3 keeps being addressed by region. Earlier releases always discarded ServiceUrl here (assigning
    // RegionEndpoint resets ServiceURL), so honouring an Amazon ServiceUrl would move working deployments to another endpoint.
    private static AmazonS3Config ForAmazonS3(S3ClaimCheckOptions options)
    {
        var config = new AmazonS3Config { ForcePathStyle = options.ForcePathStyle };
        if (!string.IsNullOrWhiteSpace(options.Region))
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);

        return config;
    }

    // Region is only the signing region for a custom endpoint. S3-compatible stores do not all accept the
    // flexible checksums AWS SDK v4 sends by default, so they keep the pre-v4 checksum behaviour.
    private static AmazonS3Config ForS3CompatibleStore(S3ClaimCheckOptions options)
    {
        var config = new AmazonS3Config
        {
            ServiceURL = options.ServiceUrl,
            ForcePathStyle = options.ForcePathStyle,
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED
        };
        if (!string.IsNullOrWhiteSpace(options.Region))
            config.AuthenticationRegion = options.Region;

        return config;
    }
}
