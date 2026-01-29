# MongoBus.ClaimCheck.AzureBlob

Azure Blob Storage claim check provider for MongoBus.

## Install

```
dotnet add package MongoBus.ClaimCheck.AzureBlob
```

## Usage

```csharp
services.AddMongoBusClaimCheckAzureBlob(opt =>
{
    opt.ConnectionString = "<storage-connection-string>";
    opt.ContainerName = "mongobus-claimcheck";
    // opt.BlobPrefix = "optional/prefix";
});
```

## License

MIT
