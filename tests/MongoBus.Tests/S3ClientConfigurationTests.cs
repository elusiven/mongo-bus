using Amazon;
using Amazon.Runtime;
using FluentAssertions;
using MongoBus.ClaimCheck;
using MongoBus.DependencyInjection;
using Xunit;

namespace MongoBus.Tests;

public class S3ClientConfigurationTests
{
    [Fact]
    public void Create_WithS3CompatibleServiceUrl_ShouldUseServiceUrlAndRegionForSigningOnly()
    {
        var options = Options(serviceUrl: "https://s3.us-east-1.wasabisys.com", region: "us-east-1");

        var config = S3ClientConfiguration.Create(options);

        config.ServiceURL.Should().StartWith("https://s3.us-east-1.wasabisys.com");
        config.AuthenticationRegion.Should().Be("us-east-1");
        config.RequestChecksumCalculation.Should().Be(RequestChecksumCalculation.WHEN_REQUIRED);
        config.ResponseChecksumValidation.Should().Be(ResponseChecksumValidation.WHEN_REQUIRED);
    }

    [Theory]
    [InlineData("")]
    [InlineData("https://s3.amazonaws.com")]
    [InlineData("https://s3.eu-west-1.amazonaws.com")]
    [InlineData("https://s3.cn-north-1.amazonaws.com.cn")]
    public void Create_WithoutS3CompatibleServiceUrl_ShouldAddressAmazonS3ByRegionWithDefaultChecksums(string serviceUrl)
    {
        var options = Options(serviceUrl, region: "eu-west-1");

        var config = S3ClientConfiguration.Create(options);

        config.RegionEndpoint.Should().Be(RegionEndpoint.EUWest1);
        config.ServiceURL.Should().BeNull();
        config.RequestChecksumCalculation.Should().Be(RequestChecksumCalculation.WHEN_SUPPORTED);
        config.ResponseChecksumValidation.Should().Be(ResponseChecksumValidation.WHEN_SUPPORTED);
    }

    private static S3ClaimCheckOptions Options(string serviceUrl, string region) => new()
    {
        AccessKey = "access-key",
        SecretKey = "secret-key",
        BucketName = "bucket",
        ServiceUrl = serviceUrl,
        Region = region
    };
}
