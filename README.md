# MongoBus

<img width="300" height="300" alt="ChatGPT Image 28 sty 2026, 12_59_12" src="https://github.com/user-attachments/assets/32751e8b-2c8d-4311-854f-c14e01704285" />

A MongoDB-backed message bus for .NET using CloudEvents for polyglot interop.

## Features

- **MongoDB Inbox Pattern**: Reliable message delivery using MongoDB as the storage and coordination layer.
- **CloudEvents Compliance**: Messages are wrapped in CloudEvents-compliant envelopes for standardized polyglot interop.
- **Optimized Performance**: Uses cached delegates for message dispatching instead of slow reflection on every message.
- **Competing Consumers**: Native support for horizontal scaling; multiple instances can process messages in parallel without duplicates.
- **Fan-out Support**: A single published message can be delivered to multiple distinct endpoints/consumers.
- **Configurable Retries**: Exponential backoff with per-consumer configurable retry limits.
- **Dead Letter Handling**: Failed messages that exceed retry limits are marked as `Dead` with full exception details stored for debugging.
- **Automatic Index Management**: Self-healing index creation on startup, including TTL indexes for automatic cleanup of processed messages (configurable, default 7 days).
- **Default Source**: Configure a default `source` for all published messages at the bus level.
- **Message Correlation**: Automatic propagation of `CorrelationId` and `CausationId` across message flows for better traceability.
- **Idempotency**: Built-in support for message deduplication at the consumer level based on CloudEvent `id`.
- **Delayed Messages**: Support for scheduled message delivery (e.g., `PublishAsync(..., deliverAt: dateTime)`).
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

- [ ] **Message Idempotency (Outbox)**: Support for transactional outbox pattern.
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
