# MongoBus

<img width="300" height="300" alt="ChatGPT Image 28 sty 2026, 12_59_12" src="https://github.com/user-attachments/assets/32751e8b-2c8d-4311-854f-c14e01704285" />

A MongoDB-backed message bus for .NET using CloudEvents for polyglot interop.

## Features

- **MongoDB Inbox Pattern**: Reliable message delivery using MongoDB as the storage and coordination layer.
- **CloudEvents Compliance**: Messages are wrapped in CloudEvents-compliant envelopes for standardized polyglot interop.
- **Optimized Performance**: Uses cached delegates for message dispatching instead of slow reflection on every message.
- **Competing Consumers**: Native support for horizontal scaling; multiple instances can process messages in parallel without duplicates.
- **Batch Consumers**: Process messages in configurable batches with size and time-based flush controls.
- **Fan-out Support**: A single published message can be delivered to multiple distinct endpoints/consumers.
- **Configurable Retries**: Exponential backoff with per-consumer configurable retry limits.
- **Dead Letter Handling**: Failed messages that exceed retry limits are marked as `Dead` with full exception details stored for debugging.
- **Automatic Index Management**: Self-healing index creation on startup, including TTL indexes for automatic cleanup of processed messages (configurable, default 7 days).
- **Default Source**: Configure a default `source` for all published messages at the bus level.
- **Message Correlation**: Automatic propagation of `CorrelationId` and `CausationId` across message flows for better traceability.
- **Idempotency**: Built-in support for message deduplication at the consumer level based on CloudEvent `id`.
- **Delayed Messages**: Support for scheduled message delivery (e.g., `PublishAsync(..., deliverAt: dateTime)`).
- **Claim Check Pattern**: Automatically offload large message payloads to external blob storage and retrieve them on consume.
- **Middleware Support**: Ability to plug in custom logic for publishing and consuming pipelines (Interceptors).
- **OpenTelemetry Support**: Native integration for distributed tracing with automatic context propagation.
- **Monitoring UI**: A dashboard to visualize message rates, failures, and dead-lettered messages.
- **Type Safety**: Strong typing for messages and handlers while maintaining loose coupling via `TypeId` strings.

## Getting Started

### Installation

Add the `MongoBus` project reference or install the NuGet package:

```bash
# Example if using NuGet
dotnet add package MongoBus
```

### Configuration

Register MongoBus in your `IServiceCollection`:

```csharp
builder.Services.AddMongoBus(opt =>
{
    opt.ConnectionString = "mongodb://localhost:27017";
    opt.DatabaseName = "MyMessageBus";
    opt.DefaultSource = "my-awesome-service"; // Optional: set a default source for all publishes
    opt.ProcessedMessageTtl = TimeSpan.FromDays(14); // Optional: change cleanup policy
});
```

### Defining Messages and Handlers

```csharp
// 1. Define your message model
public record OrderCreated(string OrderId, decimal Amount);

// 2. Implement the handler
public class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, ConsumeContext context, CancellationToken ct)
    {
        logger.LogInformation("Processing order {OrderId}", message.OrderId);
        return Task.CompletedTask;
    }
}

// 3. Define the consumer behavior
public class OrderCreatedDefinition : ConsumerDefinition<OrderCreatedHandler, OrderCreated>
{
    public override string TypeId => "orders.v1.created"; // CloudEvent type
    public override string EndpointName => "order-processing"; // MongoDB collection/queue name (Optional - set to consumer's name by default)
    
    // Optional overrides:
    public override int ConcurrencyLimit => 10;
    public override int MaxAttempts => 5;
    public override bool IdempotencyEnabled => true; // Enable deduplication
    public override TimeSpan LockTime => TimeSpan.FromMinutes(2);
}
```

### Batch Consumers

Batch consumers allow you to process multiple messages at once. You can control batch size, how long to wait since the *first* message, and how long to wait since the *last* message.

```csharp
public record OrderBatch(string OrderId, decimal Amount);

public class OrderBatchHandler(ILogger<OrderBatchHandler> logger) : IBatchMessageHandler<OrderBatch>
{
    public Task HandleBatchAsync(IReadOnlyList<OrderBatch> messages, BatchConsumeContext context, CancellationToken ct)
    {
        logger.LogInformation("Processing batch of {Count} orders", messages.Count);
        return Task.CompletedTask;
    }
}

public class OrderBatchDefinition : BatchConsumerDefinition<OrderBatchHandler, OrderBatch>
{
    public override string TypeId => "orders.v1.batch";
    public override string EndpointName => "order-batch";
    public override int ConcurrencyLimit => 2;
    public override BatchConsumerOptions BatchOptions => new()
    {
        MinBatchSize = 1,
        MaxBatchSize = 100,
        MaxBatchWaitTime = TimeSpan.FromSeconds(2),    // from first message
        MaxBatchIdleTime = TimeSpan.FromMilliseconds(200), // from last message
        FailureMode = BatchFailureMode.RetryBatch
    };
}

builder.Services.AddMongoBusBatchConsumer<OrderBatchHandler, OrderBatch, OrderBatchDefinition>();
```

### Registering Consumers

```csharp
builder.Services.AddMongoBusConsumer<OrderCreatedHandler, OrderCreated, OrderCreatedDefinition>();
```

### Publishing Messages

```csharp
var bus = serviceProvider.GetRequiredService<IMessageBus>();

// Uses the configured DefaultSource
await bus.PublishAsync("orders.v1.created", new OrderCreated("ORD-123", 99.99m));

// Or override the source explicitly
await bus.PublishAsync("orders.v1.created", new OrderCreated("ORD-456", 10.00m), source: "legacy-system");

// Publish a delayed message
await bus.PublishAsync("orders.v1.created", new OrderCreated("ORD-789", 50.00m), deliverAt: DateTime.UtcNow.AddMinutes(30));

// Force claim-check even if message is small or globally disabled
await bus.PublishAsync("orders.v1.created", new OrderCreated("ORD-000", 1.00m), useClaimCheck: true);

// Note: If claim-check is enabled globally, large messages will ALWAYS use claim-check 
// even if you pass useClaimCheck: false.
await bus.PublishAsync("orders.v1.created", new OrderCreated("ORD-999", 999999.99m), useClaimCheck: false);
```

### Middleware / Interceptors

You can hook into the publishing and consuming pipelines by implementing `IPublishInterceptor` and `IConsumeInterceptor`.

```csharp
public class MyPublishInterceptor : IPublishInterceptor
{
    public async Task OnPublishAsync<T>(PublishContext<T> context, Func<Task> next, CancellationToken ct)
    {
        // Logic before publish
        await next();
        // Logic after publish
    }
}

// Register them:
builder.Services.AddMongoBusPublishInterceptor<MyPublishInterceptor>();
builder.Services.AddMongoBusConsumeInterceptor<MyConsumeInterceptor>();
```

### Claim Check (Large Messages)

MongoBus can automatically offload large message payloads to external blob storage (Claim Check pattern). When enabled, the bus stores a lightweight reference in MongoDB and retrieves the full payload during consumption.

#### TTL and Cleanup

By default, `InboxMessage` documents are automatically cleaned up from MongoDB after they are processed (default 7 days after creation). To prevent storage leaks, MongoBus also manages the cleanup of offloaded payloads:

- **Automatic Cleanup**: A background service (`ClaimCheckCleanupService`) periodically identifies and deletes offloaded payloads that are no longer referenced by any message in the system.
- **TTL for GridFS**: When using MongoDB GridFS, a native TTL index is automatically created on the files collection to ensure data is removed even if the background service is not running.
- **Configuration**: You can tune the cleanup interval and safety margin:
  ```csharp
  services.AddMongoBus(opt => {
      opt.ClaimCheck.Cleanup.Enabled = true;
      opt.ClaimCheck.Cleanup.Interval = TimeSpan.FromHours(6);
      opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.FromDays(8); // Should be > ProcessedMessageTtl
  });
  ```

#### Enable Claim Check

```csharp
builder.Services.AddMongoBus(opt =>
{
    opt.ConnectionString = "mongodb://localhost:27017";
    opt.DatabaseName = "MyMessageBus";
    opt.ClaimCheck.Enabled = true;
    opt.ClaimCheck.ThresholdBytes = 256 * 1024; // 256 KB
    opt.ClaimCheck.ProviderName = "azure"; // or "wasabi" or a custom provider name
    
    // Optional: Enable automatic compression
    opt.ClaimCheck.Compression.Enabled = true; 
    opt.ClaimCheck.Compression.Algorithm = "gzip"; 
});
```

#### MongoDB GridFS

```csharp
builder.Services.AddMongoBusGridFsClaimCheck(bucketName: "my-claims");
```

#### Azure Blob Storage

```csharp
builder.Services.AddMongoBusClaimCheckAzureBlob(opt =>
{
    opt.ConnectionString = "<azure-blob-connection-string>";
    opt.ContainerName = "mongobus-claimcheck";
    opt.BlobPrefix = "payloads/";
});
```

#### S3-Compatible Storage (Wasabi, AWS S3, MinIO)

```csharp
builder.Services.AddMongoBusClaimCheckS3(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.us-east-1.wasabisys.com"; // or AWS S3 URL
    opt.BucketName = "mongobus-claimcheck";
    opt.KeyPrefix = "payloads/";
    opt.Region = "us-east-1";
});
```

Or use the Wasabi-specific helper:

```csharp
builder.Services.AddMongoBusClaimCheckWasabi(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.us-east-1.wasabisys.com";
    opt.BucketName = "mongobus-claimcheck";
});
```

#### Custom Provider

Implement `IClaimCheckProvider` and register it:

```csharp
builder.Services.AddMongoBusClaimCheckProvider<MyClaimCheckProvider>();
```

> Note: If your handler uses `Stream` as the message type, the stream is passed through directly and must be disposed by the handler.

### OpenTelemetry

MongoBus has native support for OpenTelemetry. To enable it, simply add the `MongoBus` activity source to your tracer provider:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing =>
    {
        tracing.AddSource("MongoBus")
               .AddConsoleExporter();
    });
```

### Monitoring Dashboard

MongoBus provides a monitoring dashboard that can be integrated into your ASP.NET Core application.

1. Install the `MongoBus.Dashboard` NuGet package.
2. Register the dashboard services:

```csharp
builder.Services.AddMongoBusDashboard();
```

3. Map the dashboard endpoints in your application pipeline:

```csharp
app.MapMongoBusDashboard(); // Defaults to /mongobus
// or with a custom pattern
app.MapMongoBusDashboard("/my-monitoring");
```

The dashboard provides real-time (polling) updates for:
- Overall message status counts (Pending, Processed, Dead).
- Per-endpoint statistics.
- Detailed logs of the most recent failures.

## Roadmap

Future improvements planned for MongoBus:

- [x] **Batch Consumer**: Support for handling messages in batches with configurable size and timing options.
- [ ] **Message Idempotency (Outbox)**: Support for transactional outbox pattern.
- [ ] **Distributed Transactions (SAGAs)**: Support for distributed transactions.
- [ ] **Monitoring Dashboard Improvements**: Real-time updates via WebSockets/SignalR and historical charts.

## Samples

Check out the `samples/MongoBus.Sample` project for a complete working example showcasing:
- Multiple message types.
- Custom endpoint naming.
- Publishing and consuming flow.
- Structured logging.

## Testing

The project includes an integration test suite using **Testcontainers.MongoDb**. To run tests:

```bash
dotnet test
```
