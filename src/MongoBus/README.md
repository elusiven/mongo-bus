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

// MongoBus validates configuration at startup and throws if invalid.

services.AddMongoBusConsumer<MyHandler, MyMessage, MyDefinition>();
services.AddMongoBusBatchConsumer<MyBatchHandler, MyMessage, MyBatchDefinition>();
services.AddMongoBusInMemoryClaimCheck();

var sp = services.BuildServiceProvider();
var bus = sp.GetRequiredService<IMessageBus>();
await bus.PublishAsync("my.message", new MyMessage());
```

For more information visit project github

## Batch consumers

```csharp
public class MyBatchHandler : IBatchMessageHandler<MyMessage>
{
    public Task HandleBatchAsync(IReadOnlyList<MyMessage> messages, BatchConsumeContext context, CancellationToken ct)
        => Task.CompletedTask;
}

public class MyBatchDefinition : BatchConsumerDefinition<MyBatchHandler, MyMessage>
{
    public override string TypeId => "my.message.batch";
    public override BatchConsumerOptions BatchOptions => new()
    {
        MinBatchSize = 5,
        MaxBatchSize = 100,
        MaxBatchWaitTime = TimeSpan.FromSeconds(2),
        MaxBatchIdleTime = TimeSpan.Zero,
        FlushMode = BatchFlushMode.SinceFirstMessage,
        MaxInFlightBatches = 1
    };

    public override IBatchGroupingStrategy GroupingStrategy =>
        BatchGrouping.ByMetadata(ctx => ctx.Subject ?? "default");
}
```

## Observers (Publish/Consume/Batch)

```csharp
public sealed class MyBatchObserver : IBatchObserver
{
    public void OnBatchProcessed(BatchMetrics metrics) { }
    public void OnBatchFailed(BatchFailureMetrics metrics) { }
}

public sealed class MyPublishObserver : IPublishObserver
{
    public void OnPublish(PublishMetrics metrics) { }
    public void OnPublishFailed(PublishFailureMetrics metrics) { }
}

public sealed class MyConsumeObserver : IConsumeObserver
{
    public void OnMessageProcessed(ConsumeMetrics metrics) { }
    public void OnMessageFailed(ConsumeFailureMetrics metrics) { }
}

services.AddMongoBusBatchObserver<MyBatchObserver>();
services.AddMongoBusPublishObserver<MyPublishObserver>();
services.AddMongoBusConsumeObserver<MyConsumeObserver>();
```

## Claim check providers

Core package includes in-memory claim check only. For other providers install:
- MongoBus.ClaimCheck.AzureBlob
- MongoBus.ClaimCheck.S3
- MongoBus.ClaimCheck.GridFs

## License

MIT
