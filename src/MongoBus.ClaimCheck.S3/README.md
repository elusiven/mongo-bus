# MongoBus.ClaimCheck.S3

S3-compatible claim check provider for MongoBus (AWS S3, MinIO, Wasabi, etc.).

## Install

```
dotnet add package MongoBus.ClaimCheck.S3
```

## Usage

```csharp
services.AddMongoBusClaimCheckS3(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.amazonaws.com";
    opt.BucketName = "mongobus-claimcheck";
    // opt.Region = "us-east-1";
    // opt.KeyPrefix = "optional/prefix";
});

// Or Wasabi
services.AddMongoBusClaimCheckWasabi(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.wasabisys.com";
    opt.BucketName = "mongobus-claimcheck";
});
```

## License

MIT
