# MongoBus

MongoDB-backed message bus using CloudEvents envelope (polyglot interop).

## Install

```
dotnet add package MongoBus
```

## Quick start

```csharp
var services = new ServiceCollection();
services.AddLogging();
services.AddMongoBus(opt =>
{
    opt.ConnectionString = "mongodb://localhost:27017";
    opt.DatabaseName = "mongo_bus";
});

services.AddMongoBusConsumer<MyHandler, MyMessage, MyDefinition>();
services.AddMongoBusInMemoryClaimCheck();

var sp = services.BuildServiceProvider();
var bus = sp.GetRequiredService<IMessageBus>();
await bus.PublishAsync("my.message", new MyMessage());
```

For more information visit project github

## Claim check providers

Core package includes in-memory claim check only. For other providers install:
- MongoBus.ClaimCheck.AzureBlob
- MongoBus.ClaimCheck.S3
- MongoBus.ClaimCheck.GridFs

## License

MIT
