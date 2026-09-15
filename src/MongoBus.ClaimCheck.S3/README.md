# MongoBus.ClaimCheck.S3

S3-compatible claim check provider for MongoBus (AWS S3, MinIO, Wasabi, etc.).

## Install

```
dotnet add package MongoBus.ClaimCheck.S3
```

## Usage

```csharp
// AWS S3: addressed by Region
services.AddMongoBusClaimCheckS3(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.Region = "eu-west-1";
    opt.BucketName = "mongobus-claimcheck";
    // opt.KeyPrefix = "optional/prefix";
});

// S3-compatible store (MinIO, Ceph, ...): addressed by ServiceUrl, Region is the signing region
services.AddMongoBusClaimCheckS3(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "http://localhost:9000";
    opt.Region = "us-east-1";
    opt.BucketName = "mongobus-claimcheck";
});

// Or Wasabi
services.AddMongoBusClaimCheckWasabi(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.us-east-1.wasabisys.com";
    opt.Region = "us-east-1";
    opt.BucketName = "mongobus-claimcheck";
});
```

## Endpoint selection

- A `ServiceUrl` on an Amazon host (`*.amazonaws.com`, `*.amazonaws.com.cn`), or no `ServiceUrl`, means AWS S3. The client is addressed by `Region` (or the region from the environment/AWS profile) and keeps the AWS SDK's default request checksums.
- Any other `ServiceUrl` is used as the endpoint, with `Region` as the signing region. Request checksums are only sent when an operation requires them, because not every S3-compatible store supports the flexible checksums AWS SDK v4 sends by default.

## License

MIT
