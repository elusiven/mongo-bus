using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using MongoBus.Dashboard;

var builder = WebApplication.CreateBuilder(args);

// 1. Configure MongoBus
builder.Services.AddMongoBus(opt =>
{
    opt.ConnectionString = "mongodb://localhost:27017";
    opt.DatabaseName = "SampleBusDb";
    opt.DefaultSource = "sample-app";
    opt.ProcessedMessageTtl = TimeSpan.FromDays(14);

    opt.ClaimCheck.Enabled = true;
    opt.ClaimCheck.ThresholdBytes = 128;
    opt.ClaimCheck.ProviderName = "memory";
    opt.ClaimCheck.Compression.Enabled = true;
    opt.ClaimCheck.Compression.Algorithm = "gzip";
    opt.ClaimCheck.Cleanup.Interval = TimeSpan.FromMinutes(10);
    opt.ClaimCheck.Cleanup.MinimumAge = TimeSpan.FromDays(15);
});

// 2. Configure Dashboard
builder.Services.AddMongoBusDashboard();

// 3. Register Consumers
builder.Services.AddMongoBusConsumer<OrderCreatedHandler, OrderCreated, OrderCreatedDefinition>();
builder.Services.AddMongoBusConsumer<EmailNotificationHandler, EmailNotification, EmailNotificationDefinition>();
builder.Services.AddMongoBusBatchConsumer<OrderBatchHandler, OrderBatch, OrderBatchDefinition>();

builder.Services.AddMongoBusPublishInterceptor<SamplePublishInterceptor>();
builder.Services.AddMongoBusConsumeInterceptor<SampleConsumeInterceptor>();
builder.Services.AddMongoBusBatchConsumeInterceptor<SampleBatchInterceptor>();

builder.Services.AddMongoBusPublishObserver<SamplePublishObserver>();
builder.Services.AddMongoBusConsumeObserver<SampleConsumeObserver>();
builder.Services.AddMongoBusBatchObserver<SampleBatchObserver>();

builder.Services.AddMongoBusInMemoryClaimCheck();

var app = builder.Build();

// 4. Map Dashboard
app.MapMongoBusDashboard();

// 5. Start the app
await app.StartAsync();

var bus = app.Services.GetRequiredService<IMessageBus>();
var logger = app.Services.GetRequiredService<ILogger<Program>>();

logger.LogInformation("Sample application started with Dashboard at /mongobus. Publishing messages...");

// 4. Publish some messages
await bus.PublishAsync("orders.created", new OrderCreated 
{ 
    OrderId = Guid.NewGuid().ToString("N"), 
    CustomerName = "John Doe", 
    Amount = 99.99m 
}, correlationId: "corr-001");

// Idempotency demo: same CloudEvent id twice (second should be skipped)
await bus.PublishAsync("orders.created", new OrderCreated
{
    OrderId = "DUP-001",
    CustomerName = "Jane Doe",
    Amount = 55.00m
}, id: "fixed-id-1");

await bus.PublishAsync("orders.created", new OrderCreated
{
    OrderId = "DUP-001",
    CustomerName = "Jane Doe",
    Amount = 55.00m
}, id: "fixed-id-1");

await bus.PublishAsync("notifications.email", new EmailNotification 
{ 
    Recipient = "john.doe@example.com", 
    Subject = "Your order has been placed", 
    Body = "Thank you for your order!" 
});

await bus.PublishAsync("notifications.email", new EmailNotification
{
    Recipient = "delayed@example.com",
    Subject = "Delayed Notification",
    Body = "This message was delayed by 10 seconds."
}, deliverAt: DateTime.UtcNow.AddSeconds(10));

// Claim-check demo (payload > threshold)
await bus.PublishAsync("notifications.email", new EmailNotification
{
    Recipient = "bigpayload@example.com",
    Subject = "Large Payload",
    Body = new string('x', 1000)
});

// Batch messages (grouped by customer id)
var customerA = "customer-a";
var customerB = "customer-b";

for (var i = 0; i < 5; i++)
{
    await bus.PublishAsync("orders.batch", new OrderBatch
    {
        CustomerId = i % 2 == 0 ? customerA : customerB,
        OrderId = $"BATCH-{i:000}",
        Amount = 10 + i
    }, subject: i % 2 == 0 ? "priority" : "standard");
}

logger.LogInformation("Messages published. Waiting for processing... (Press Ctrl+C to exit)");

// Keep the application running to allow the background worker to process messages
await app.WaitForShutdownAsync();

// --- Message Models ---

public record OrderCreated
{
    public string OrderId { get; init; } = "";
    public string CustomerName { get; init; } = "";
    public decimal Amount { get; init; }
}

public record EmailNotification
{
    public string Recipient { get; init; } = "";
    public string Subject { get; init; } = "";
    public string Body { get; init; } = "";
}

public record OrderBatch
{
    public string CustomerId { get; init; } = "";
    public string OrderId { get; init; } = "";
    public decimal Amount { get; init; }
}

// --- Handlers ---

public class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, ConsumeContext context, CancellationToken ct)
    {
        logger.LogInformation("Processing OrderCreated: {OrderId} for {CustomerName} (${Amount})", 
            message.OrderId, message.CustomerName, message.Amount);
        
        return Task.CompletedTask;
    }
}

public class EmailNotificationHandler(ILogger<EmailNotificationHandler> logger) : IMessageHandler<EmailNotification>
{
    public Task HandleAsync(EmailNotification message, ConsumeContext context, CancellationToken ct)
    {
        logger.LogInformation("Sending Email to {Recipient}: {Subject}", 
            message.Recipient, message.Subject);
        return Task.CompletedTask;
    }
}

public class OrderBatchHandler(ILogger<OrderBatchHandler> logger) : IBatchMessageHandler<OrderBatch>
{
    public Task HandleBatchAsync(IReadOnlyList<OrderBatch> messages, BatchConsumeContext context, CancellationToken ct)
    {
        logger.LogInformation("Processing batch {Group} with {Count} orders", context.GroupKey, messages.Count);
        return Task.CompletedTask;
    }
}

public sealed class SamplePublishInterceptor : IPublishInterceptor
{
    public Task OnPublishAsync<T>(PublishContext<T> context, Func<Task> next, CancellationToken ct)
        => next();
}

public sealed class SampleConsumeInterceptor : IConsumeInterceptor
{
    public Task OnConsumeAsync(ConsumeContext context, object message, Func<Task> next, CancellationToken ct)
        => next();
}

public sealed class SampleBatchInterceptor : IBatchConsumeInterceptor
{
    public Task OnConsumeBatchAsync(BatchConsumeContext context, IReadOnlyList<object> messages, Func<Task> next, CancellationToken ct)
        => next();
}

public sealed class SamplePublishObserver(ILogger<SamplePublishObserver> logger) : IPublishObserver
{
    public void OnPublish(PublishMetrics metrics) =>
        logger.LogInformation("Published {TypeId} to {Endpoints} endpoints in {Latency}ms", metrics.TypeId, metrics.EndpointCount, metrics.Latency.TotalMilliseconds);

    public void OnPublishFailed(PublishFailureMetrics metrics) =>
        logger.LogWarning(metrics.Exception, "Publish failed for {TypeId}", metrics.TypeId);
}

public sealed class SampleConsumeObserver(ILogger<SampleConsumeObserver> logger) : IConsumeObserver
{
    public void OnMessageProcessed(ConsumeMetrics metrics) =>
        logger.LogInformation("Consumed {TypeId} in {Latency}ms", metrics.Context.TypeId, metrics.Latency.TotalMilliseconds);

    public void OnMessageFailed(ConsumeFailureMetrics metrics) =>
        logger.LogWarning(metrics.Exception, "Consume failed for {TypeId}", metrics.Context.TypeId);
}

public sealed class SampleBatchObserver(ILogger<SampleBatchObserver> logger) : IBatchObserver
{
    public void OnBatchProcessed(BatchMetrics metrics) =>
        logger.LogInformation("Batch processed {TypeId} size {Size} group {Group}", metrics.TypeId, metrics.BatchSize, metrics.GroupKey);

    public void OnBatchFailed(BatchFailureMetrics metrics) =>
        logger.LogWarning(metrics.Exception, "Batch failed {TypeId} size {Size}", metrics.TypeId, metrics.BatchSize);
}

// --- Definitions ---

public class OrderCreatedDefinition : ConsumerDefinition<OrderCreatedHandler, OrderCreated>
{
    public override string TypeId => "orders.created";
    public override string EndpointName => "order-processing-service";
    public override int ConcurrencyLimit => 4;
    public override int PrefetchCount => 16;
    public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
    public override int MaxAttempts => 3;
    public override bool IdempotencyEnabled => true;
}

public class EmailNotificationDefinition : ConsumerDefinition<EmailNotificationHandler, EmailNotification>
{
    public override string TypeId => "notifications.email";
    // Using default EndpointName (will be email-notification)
    public override int ConcurrencyLimit => 2;
    public override int PrefetchCount => 8;
}

public class OrderBatchDefinition : BatchConsumerDefinition<OrderBatchHandler, OrderBatch>
{
    public override string TypeId => "orders.batch";
    public override string EndpointName => "order-batch";
    public override int ConcurrencyLimit => 2;
    public override int MaxAttempts => 5;
    public override bool IdempotencyEnabled => true;

    public override BatchConsumerOptions BatchOptions => new()
    {
        MinBatchSize = 1,
        MaxBatchSize = 10,
        MaxBatchWaitTime = TimeSpan.FromSeconds(2),
        MaxBatchIdleTime = TimeSpan.Zero,
        FlushMode = BatchFlushMode.SinceFirstMessage,
        FailureMode = BatchFailureMode.RetryBatch,
        MaxInFlightBatches = 2
    };

    public override IBatchGroupingStrategy GroupingStrategy =>
        BatchGrouping.ByMessage<OrderBatch>(m => m.CustomerId);
}
