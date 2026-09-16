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
- **Automatic Index Management**: Self-healing index creation on startup, including TTL indexes that remove messages a configurable time after they are processed (default 7 days). Pending, delayed and dead-lettered messages are never expired.
- **Default Source**: Configure a default `source` for all published messages at the bus level.
- **Message Correlation**: Automatic propagation of `CorrelationId` and `CausationId` across message flows for better traceability.
- **Idempotency**: Built-in support for message deduplication at the consumer level based on CloudEvent `id`.
- **Delayed Messages**: Support for scheduled message delivery (e.g., `PublishAsync(..., deliverAt: dateTime)`).
- **Claim Check Pattern**: Automatically offload large message payloads to external blob storage and retrieve them on consume.
- **Middleware Support**: Ability to plug in custom logic for publishing and consuming pipelines (Interceptors).
- **OpenTelemetry Support**: Native integration for distributed tracing with automatic context propagation.
- **Saga State Machines**: Declarative state machines for orchestrating long-running workflows with optimistic concurrency, history/audit log, and timeout support.
- **Monitoring UI**: A dashboard to visualize message rates, failures, dead-lettered messages, and saga instances.
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

// MongoBus validates configuration at startup and throws if invalid.
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
    public override bool RenewLock => true; // Keep the lock while a long handler runs; its token is cancelled if the lock is lost
}
```

#### Long-running handlers

A handler that can outlive `LockTime` sets `RenewLock => true` (with a `LockTime` of at least one second). The lock is then extended every third of `LockTime` for as long as the handler runs — including while the bus is stopping — so no other consumer picks the message up, and if the consumer crashes the message is redelivered once the lock lapses. If the lock is lost — another consumer took the message, or renewals did not succeed before the lock neared expiry — the handler's `CancellationToken` is cancelled; stop promptly, because the message may already be running elsewhere. `RenewLock` applies to every consumer on the same endpoint. Batch consumers do not renew locks.

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
        MaxBatchWaitTime = TimeSpan.FromSeconds(2),    // used when FlushMode=SinceFirstMessage
        MaxBatchIdleTime = TimeSpan.Zero,              // must be 0 when FlushMode=SinceFirstMessage
        FlushMode = BatchFlushMode.SinceFirstMessage,
        FailureMode = BatchFailureMode.RetryBatch,
        MaxInFlightBatches = 2
    };

    // Group by message property or metadata
    public override IBatchGroupingStrategy GroupingStrategy =>
        BatchGrouping.ByMessage<OrderBatch>(m => m.OrderId);
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

### Transactional Outbox

MongoBus supports publishing to an outbox so your business write and event publish can be committed together.

```csharp
// Configuration
builder.Services.AddMongoBus(opt =>
{
    opt.ConnectionString = "mongodb://localhost:27017";
    opt.DatabaseName = "MyMessageBus";

    opt.Outbox.Enabled = true;

    // Optional: route selected types through outbox when using IMessageBus.PublishAsync
    opt.UseOutboxForTypeId = typeId => typeId.StartsWith("orders.");
});

// Explicit outbox publishing API
var txBus = serviceProvider.GetRequiredService<ITransactionalMessageBus>();
var mongoClient = serviceProvider.GetRequiredService<IMongoClient>();
var db = serviceProvider.GetRequiredService<IMongoDatabase>();

await txBus.PublishWithTransactionAsync(
    "orders.v1.created",
    new OrderCreated("ORD-TX-001", 149.99m),
    async (session, ct) =>
    {
        var orders = db.GetCollection<BsonDocument>("orders");
        await orders.InsertOneAsync(
            session,
            new BsonDocument
            {
                ["orderId"] = "ORD-TX-001",
                ["amount"] = 149.99m,
                ["createdUtc"] = DateTime.UtcNow
            },
            cancellationToken: ct);
    });

// Or just enqueue to outbox (without opening a transaction explicitly)
await txBus.PublishToOutboxAsync(
    "orders.v1.created",
    new OrderCreated("ORD-OUTBOX-001", 39.99m));
```

> Note: MongoDB transactions require a replica set or sharded cluster.
> For a complete runnable demo, see `samples/MongoBus.Sample/Program.cs`.

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

### Observers (Publish/Consume/Batch)

Observers are lightweight hooks for telemetry and auditing (not middleware). MongoBus registers default OpenTelemetry observers for publish, consume, and batch processing. You can add your own:

```csharp
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

public sealed class MyBatchObserver : IBatchObserver
{
    public void OnBatchProcessed(BatchMetrics metrics) { }
    public void OnBatchFailed(BatchFailureMetrics metrics) { }
}

builder.Services.AddMongoBusPublishObserver<MyPublishObserver>();
builder.Services.AddMongoBusConsumeObserver<MyConsumeObserver>();
builder.Services.AddMongoBusBatchObserver<MyBatchObserver>();
```

### Claim Check (Large Messages)

MongoBus can automatically offload large message payloads to external blob storage (Claim Check pattern). When enabled, the bus stores a lightweight reference in MongoDB and retrieves the full payload during consumption.

#### TTL and Cleanup

By default, `InboxMessage` documents are automatically removed from MongoDB 7 days after they are processed (`ProcessedMessageTtl`, enforced by a TTL index on `ProcessedUtc`). Pending, delayed and dead-lettered messages are kept until they are handled. To prevent storage leaks, MongoBus also manages the cleanup of offloaded payloads:

- **Automatic Cleanup**: A background service (`ClaimCheckCleanupService`) periodically deletes offloaded payloads older than `MinimumAge` that no message in `bus_inbox` or `bus_outbox` references, so payloads of delayed outbox messages are kept until they are relayed. It runs whenever a claim-check provider is registered and `Cleanup.Enabled` is true, even when `ClaimCheck.Enabled` is false, because a message can request a claim check on its own (`useClaimCheck: true`). This applies to every provider, including GridFS.
- **Shared storage**: Cleanup only considers payloads MongoBus stored: Azure blobs under `BlobPrefix` that carry MongoBus metadata, S3 objects under `KeyPrefix` whose name is the prefix followed by a GUID, and GridFS files that carry MongoBus metadata. Objects other applications keep in the same bucket or container are left alone. Give each bus its own bucket, container or prefix: a bus deletes payloads its own database does not reference. Publishers using another client must use the same prefix, ending in `/`.
- **Cost**: Each run lists the stored payloads. When some are older than `MinimumAge`, it reads the claim-check messages in `bus_inbox` and `bus_outbox` once (a collection scan filtered on the payload text) and holds the keys of those expired payloads in memory while it does.
- **Upgrading**: Earlier versions expired inbox and outbox documents by `CreatedUtc` and added a TTL index to every GridFS `*.files` collection, which could delete messages and payloads that had not been handled yet. Those indexes are removed on startup.
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

Consumers read at most `ClaimCheck.Compression.MaxDecompressedBytes` (100 MiB by default) of a claim-check payload, whether or not it is compressed; for a compressed payload the limit applies to its decompressed size. A message whose payload is larger fails instead of being handled.

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
    opt.ServiceUrl = "https://s3.us-east-1.wasabisys.com"; // omit for AWS S3
    opt.BucketName = "mongobus-claimcheck";
    opt.KeyPrefix = "payloads/";
    opt.Region = "us-east-1";
});
```

AWS S3 is addressed by `Region`; any `ServiceUrl` on an Amazon host is ignored. Other stores are addressed by `ServiceUrl`, with `Region` as the signing region. See the [provider README](src/MongoBus.ClaimCheck.S3/README.md#endpoint-selection) for details.

Or use the Wasabi-specific helper:

```csharp
builder.Services.AddMongoBusClaimCheckWasabi(opt =>
{
    opt.AccessKey = "<access-key>";
    opt.SecretKey = "<secret-key>";
    opt.ServiceUrl = "https://s3.us-east-1.wasabisys.com";
    opt.Region = "us-east-1";
    opt.BucketName = "mongobus-claimcheck";
});
```

#### Custom Provider

Implement `IClaimCheckProvider` and register it:

```csharp
builder.Services.AddMongoBusClaimCheckProvider<MyClaimCheckProvider>();
```

`ListAsync` must return only the payloads your provider stored: cleanup deletes every listed payload older than `MinimumAge` that no message references.

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

The dashboard provides real-time polling updates for:
- Overall message status counts (Pending, Processed, Dead).
- Per-endpoint statistics.
- Detailed logs of the most recent failures.
- Saga instance browser with state distribution and transition history.

On startup, `AddMongoBusDashboard()` also creates an index on `bus_inbox` (`Status`, `CreatedUtc` descending) so the recent-failures list does not scan the inbox. Applications that do not register the dashboard do not get this index.

#### Securing the Dashboard

By default, the dashboard is **secured** with a built-in authorization policy that requires the `mongobus:dashboard` scope. The scope may appear anywhere in a space-separated `scope` claim, or in an `scp` claim as issued by Microsoft Entra ID (including JwtBearer's mapped `http://schemas.microsoft.com/identity/claims/scope` type). Ensure your access tokens include this scope, or override with your own policy.

```csharp
// Default: requires the mongobus:dashboard scope (no extra config needed)
builder.Services.AddMongoBusDashboard();

// Override with your own policy:
builder.Services.AddMongoBusDashboard(opt =>
    opt.AuthorizationPolicy = "MyCustomPolicy");

// Disable for development:
builder.Services.AddMongoBusDashboard(opt =>
    opt.AuthorizationPolicy = null);

// Ensure authentication/authorization middleware is registered:
app.UseAuthentication();
app.UseAuthorization();
```

All dashboard routes (UI and API) require the policy to pass. The policy is fully user-defined — use `RequireClaim` for OAuth2 scopes, `RequireRole` for role-based access, or any custom `IAuthorizationRequirement`.

### Saga State Machines

MongoBus includes a built-in saga/state machine framework for orchestrating long-running workflows.

#### Defining a Saga

```csharp
// 1. Define your saga state
public class OrderSagaState : ISagaInstance
{
    public string CorrelationId { get; set; } = default!;
    public string CurrentState { get; set; } = default!;
    public int Version { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime LastModifiedUtc { get; set; }

    // Custom properties
    public decimal OrderTotal { get; set; }
    public bool PaymentReceived { get; set; }
}

// 2. Define your state machine
public class OrderSagaStateMachine : MongoBusStateMachine<OrderSagaState>
{
    public SagaState Submitted { get; private set; }
    public SagaState PaymentPending { get; private set; }
    public SagaState Completed { get; private set; }

    public SagaEvent<OrderSubmitted> OrderSubmittedEvent { get; private set; }
    public SagaEvent<PaymentReceived> PaymentReceivedEvent { get; private set; }
    public SagaEvent<OrderCancelled> OrderCancelledEvent { get; private set; }

    public OrderSagaStateMachine()
    {
        Event(() => OrderSubmittedEvent, "orders.v1.submitted", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => PaymentReceivedEvent, "payments.v1.received", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => OrderCancelledEvent, "orders.v1.cancelled", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        InstanceState(x => x.CurrentState);

        Initially(
            When(OrderSubmittedEvent)
                .Then(ctx => ctx.Saga.OrderTotal = ctx.Message.Amount)
                .Publish("notifications.v1.order-created", ctx => new { ctx.Saga.CorrelationId })
                .TransitionTo(PaymentPending));

        During(PaymentPending,
            When(PaymentReceivedEvent)
                .Then(ctx => ctx.Saga.PaymentReceived = true)
                .TransitionTo(Completed)
                .Finalize());

        // Handle cancellation from any state
        DuringAny(
            When(OrderCancelledEvent).Finalize());

        SetCompletedWhenFinalized(); // Auto-delete completed instances
    }
}

// 3. Register the saga
builder.Services.AddMongoBusSaga<OrderSagaStateMachine, OrderSagaState>(opt =>
{
    opt.ConcurrencyLimit = 16;
    opt.HistoryEnabled = true;         // Enable audit log
    opt.HistoryTtl = TimeSpan.FromDays(30);
    opt.SagaTimeout = TimeSpan.FromHours(24); // Time out sagas created more than 24h ago
    opt.SagaInstanceTtl = TimeSpan.FromDays(7); // TTL cleanup
});
```

#### Available Activities

| Activity | Description |
|----------|-------------|
| `Then` / `ThenAsync` | Execute custom logic |
| `TransitionTo` | Change saga state |
| `Finalize` | Move to Final state |
| `Publish` / `PublishAsync` | Publish a message via the bus |
| `Send` / `SendAsync` | Send to a specific endpoint |
| `Schedule` / `ScheduleAsync` | Schedule a delayed message |
| `Unschedule` | Clear a schedule token. The scheduled message is still delivered; it is discarded if the saga has completed or its current state does not handle it |
| `Request` / `RequestAsync` | Publish request with timeout |
| `Respond` / `RespondAsync` | Publish a response |
| `If` / `IfAsync` | Conditional branching |
| `IfElse` / `IfElseAsync` | If/else branching |
| `Switch` | Multi-case branching |
| `Catch<T>` / `CatchAll` | Exception handling |

#### Saga Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `ConcurrencyLimit` | 16 | Max concurrent event handlers per saga type |
| `PrefetchCount` | 64 | Channel capacity for prefetched messages |
| `LockTime` | 60s | How long a message lock is held |
| `MaxAttempts` | 10 | Max retry attempts before dead-lettering |
| `IdempotencyEnabled` | false | Deduplicate events by CloudEvent ID |
| `HistoryEnabled` | false | Record state transitions in an audit log collection |
| `HistoryTtl` | 30 days | TTL for history entries |
| `SagaTimeout` | disabled | Move sagas created longer ago than this, and not `Final`, to `TimeoutStateName`. Age is measured from `CreatedUtc`, not from the last event |
| `TimeoutStateName` | "TimedOut" | Target state for timed-out sagas |
| `TimeoutScanInterval` | 30s | How often the timeout service scans |
| `SagaInstanceTtl` | disabled | TTL index on saga instances for auto-cleanup |
| `DefaultPartitionCount` | 0 (disabled) | Hash-based partition locking for concurrent access |
| `RetryMode` | DenyList | `DenyList` (retry all except listed) or `AllowList` (retry only listed) |
| `UseOutbox` | false | Buffer saga publishes and write them with the saga state inside one Mongo transaction. Requires a replica set / sharded cluster and `MongoBusOptions.Outbox.Enabled = true`. See "Checkpoint Granularity & Mid-Flight Failures" below. |
| `AllowFallbackWhenTransactionsUnsupported` | false | When `UseOutbox = true` but the connected deployment doesn't support transactions, fall back to direct publish (with a warning) instead of throwing. Use for dev/test against standalone Mongo. |

#### Checkpoint Granularity & Mid-Flight Failures

A saga event handler persists state **once**, at the end of the entire activity chain. Every `Then` / `ThenAsync` / `Publish` / `Send` in a `When(...)` clause runs, and only then does the runtime increment `Version` and call `UpdateAsync` on the saga instance. Two consequences follow.

**1. Intermediate writes to the saga instance are not durable until the chain finishes.** If the process crashes between activities, every mutation made to `ctx.Saga.*` during that handler is rolled back and the message redelivers. On the next attempt, the chain re-runs from activity #1.

**2. `Publish` / `Send` / `Schedule` / `Request` messages are sent only after the saga write commits.** They are buffered while the chain runs. If the post-chain `UpdateAsync` fails (Mongo blip, concurrency conflict), nothing is published, and the retry publishes once it persists the transition. Without `UseOutbox` the write and the publishes are not atomic, so a crash between the two loses the messages; `UseOutbox` writes them in the same transaction as the state.

For pure state mutations and idempotent downstream consumers this is fine. It becomes load-bearing when a single saga event handler orchestrates **multiple non-idempotent external calls** — uploading media to Meta, charging a card, provisioning a VM. A mid-handler crash re-runs every external call from the start, and the external system has no way to know it's seen the request before.

##### Pattern: one external call per state transition

The fix is to split the workflow so each external call is its own *event* with its own saga state. A worker (a regular `IMessageHandler<T>`) performs the external call and publishes a result event; the saga transitions on the result. Each transition is its own durable checkpoint.

Concretely, a 3-step Meta publish becomes four states (`UploadingMedia` → `AttachingMetadata` → `Publishing` → `Completed`) and three workers. A crash between steps 2 and 3 resumes at step 3 — step 1 and step 2 are not re-attempted.

```csharp
public class MetaPublishStateMachine : MongoBusStateMachine<MetaPublishSagaState>
{
    public SagaState UploadingMedia { get; private set; }
    public SagaState AttachingMetadata { get; private set; }
    public SagaState Publishing { get; private set; }
    public SagaState Completed { get; private set; }

    // ...event declarations...

    public MetaPublishStateMachine()
    {
        Initially(
            When(Requested)
                .Then(ctx => { ctx.Saga.MediaUrl = ctx.Message.MediaUrl; /* ... */ })
                .Publish("meta.upload.start", ctx => new StartMediaUpload(/* ... */))
                .TransitionTo(UploadingMedia));

        During(UploadingMedia,
            When(Uploaded)
                .Then(ctx => ctx.Saga.ContainerId = ctx.Message.ContainerId)
                .Publish("meta.metadata.start", ctx => /* next step */)
                .TransitionTo(AttachingMetadata));

        // ...etc...
    }
}
```

The workers are conventional consumers:

```csharp
public sealed class UploadMediaWorker(FakeMetaClient meta, IMessageBus bus)
    : IMessageHandler<StartMediaUpload>
{
    public async Task HandleAsync(StartMediaUpload msg, ConsumeContext ctx, CancellationToken ct)
    {
        // Stable idempotency token so the external API can dedupe on its side
        // if this worker retries after a partial failure.
        var key = $"{ctx.CorrelationId}:upload";
        var containerId = await meta.UploadAsync(key, msg.MediaUrl);

        await bus.PublishAsync("meta.media.uploaded",
            new MediaUploaded(containerId),
            correlationId: ctx.CorrelationId,
            causationId: ctx.CloudEventId,
            ct: ct);
    }
}
```

A runnable end-to-end version of this pattern — including a fake external client that fails on first attempt to exercise the retry path — lives in `samples/MongoBus.Sample/MultiEventSagaWorkflow.cs`.

##### When to use which

| Situation | Pattern |
|-----------|---------|
| Pure state mutations, no external side effects | Single-event saga, activities chained in one `When(...)` |
| Idempotent downstream consumers, no external HTTP calls | Single-event saga is fine |
| Multiple non-idempotent external API calls in sequence | Multi-event saga (one state per external call) |
| External API supports idempotency keys | Multi-event saga **plus** pass `{CorrelationId}:{step}` as the idempotency key |

Enabling `IdempotencyEnabled = true` on the saga options dedupes redelivered result events by CloudEvent id, which is recommended for the multi-event pattern.

##### Transactional saga publishes (`UseOutbox`)

Even without restructuring as a multi-event saga, you can close the *publish-leak* part of the problem — the case where a saga's `.Publish` / `.Send` activity fires successfully but the subsequent saga write fails, leaving an outbound message that doesn't match the saved saga state.

Set `SagaOptions.UseOutbox = true` (and `MongoBusOptions.Outbox.Enabled = true`). The runtime then:

1. Buffers every saga publish during the activity chain instead of sending it directly.
2. Opens a Mongo session and starts a transaction.
3. Writes the saga state, the history entry (if `HistoryEnabled = true`), and one outbox row per buffered publish — all inside the transaction.
4. Commits. The `MongoOutboxRelayService` picks the rows up afterwards and forwards them to the inbox.

If the commit aborts (concurrency conflict, transient Mongo error, anything else), no outbox rows survive and no publishes escape — the inbox redelivers the message and the next attempt starts from a known-clean state.

```csharp
builder.Services.AddMongoBusSaga<MetaPublishStateMachine, MetaPublishSagaState>(opt =>
{
    opt.UseOutbox = true;
});
```

Requires MongoDB with multi-document transaction support — a replica set or sharded cluster. On a standalone Mongo, the saga handler throws an explicit error on first use. For dev/test against standalone, set `AllowFallbackWhenTransactionsUnsupported = true` and the runtime falls back to direct publish with a one-time warning.

`UseOutbox` solves the publish-leak problem, but **not** the duplicate-external-call problem from non-idempotent HTTP APIs called from within the activity chain — those still need the multi-event pattern above (or external idempotency tokens). Use both together for workflows that orchestrate multiple non-idempotent external calls.

#### Saga Dashboard

The monitoring dashboard includes a **Sagas** tab with:
- Saga type browser (auto-discovered from `bus_saga_*` collections)
- State distribution badges per saga type
- Paginated instance table with state filter
- Transition history timeline (when `HistoryEnabled = true`)

API endpoints:

```
GET /mongobus/api/sagas                              — List saga collections
GET /mongobus/api/sagas/{collection}/stats            — Instance counts by state
GET /mongobus/api/sagas/{collection}/instances        — Paginated instance listing
GET /mongobus/api/sagas/{collection}/history/{id}     — Transition history
```

## Samples

Check out the `samples/MongoBus.Sample` project for a complete working example showcasing:
- Multiple message types and custom endpoint naming
- Publishing, consuming, batch consumers with grouping and backpressure
- Observers (publish/consume/batch) and interceptors
- Claim-check, delayed delivery, and transactional outbox
- **Simple saga**: OrderSubmitted → PaymentPending → Completed workflow
- **Comprehensive fulfillment saga**: 8 states, conditional routing (IfElse for high-value orders), compensation (refund on inventory failure), DuringAny cancellation, exception handling, notifications on every transition, idempotency, history, and partitioning

## Testing

The project includes an integration test suite using **Testcontainers.MongoDb**. To run tests:

```bash
dotnet test
```
