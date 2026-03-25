using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoBus.Dashboard;
using MongoDB.Bson;
using MongoDB.Driver;

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

    opt.Outbox.Enabled = true;
    opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(100);
    opt.UseOutboxForTypeId = typeId => typeId.StartsWith("orders.", StringComparison.Ordinal);
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

// 6. Register Sagas
builder.Services.AddMongoBusSaga<SampleOrderSagaStateMachine, SampleOrderSagaState>(opt =>
{
    opt.HistoryEnabled = true;
    opt.HistoryTtl = TimeSpan.FromDays(7);
    opt.SagaTimeout = TimeSpan.FromHours(1);
    opt.TimeoutScanInterval = TimeSpan.FromSeconds(30);
});

// 7. Register comprehensive fulfillment saga (demonstrates advanced features)
builder.Services.AddMongoBusSaga<OrderFulfillmentStateMachine, OrderFulfillmentState>(opt =>
{
    opt.HistoryEnabled = true;
    opt.HistoryTtl = TimeSpan.FromDays(30);
    opt.IdempotencyEnabled = true;
    opt.SagaTimeout = TimeSpan.FromHours(24);
    opt.TimeoutScanInterval = TimeSpan.FromSeconds(30);
    opt.DefaultPartitionCount = 8;
});

var app = builder.Build();

// 4. Map Dashboard
app.MapMongoBusDashboard();

// 5. Start the app
await app.StartAsync();

var bus = app.Services.GetRequiredService<IMessageBus>();
var transactionalBus = app.Services.GetRequiredService<ITransactionalMessageBus>();
var mongoClient = app.Services.GetRequiredService<IMongoClient>();
var mongoDatabase = app.Services.GetRequiredService<IMongoDatabase>();
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

// Transactional outbox demo (explicit): business write + outbox publish in one transaction.
if (await SupportsTransactionsAsync(mongoClient))
{
    await transactionalBus.PublishWithTransactionAsync(
        "orders.created",
        new OrderCreated
        {
            OrderId = "TX-ORDER-001",
            CustomerName = "Transactional Customer",
            Amount = 149.99m
        },
        async (session, ct) =>
        {
            var appOrders = mongoDatabase.GetCollection<BsonDocument>("sample_app_orders");
            var orderDoc = new BsonDocument
            {
                ["orderId"] = "TX-ORDER-001",
                ["customerName"] = "Transactional Customer",
                ["amount"] = 149.99m,
                ["createdUtc"] = DateTime.UtcNow
            };

            await appOrders.InsertOneAsync(session, orderDoc, cancellationToken: ct);
        });

    logger.LogInformation("Transactional outbox example published order TX-ORDER-001.");
}
else
{
    logger.LogInformation("Skipping PublishWithTransactionAsync demo because MongoDB transactions require a replica set/sharded cluster.");

    await transactionalBus.PublishToOutboxAsync(
        "orders.created",
        new OrderCreated
        {
            OrderId = "OUTBOX-ONLY-001",
            CustomerName = "Outbox Only Customer",
            Amount = 39.99m
        });
}

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

// Saga demo: order workflow
var sagaCorrelationId = Guid.NewGuid().ToString("N");
await bus.PublishAsync("saga.orders.submitted", new SagaOrderSubmitted
{
    OrderId = "SAGA-001",
    Amount = 249.99m
}, correlationId: sagaCorrelationId);
logger.LogInformation("Saga: Published OrderSubmitted for saga {CorrelationId}", sagaCorrelationId);

await Task.Delay(1000);

await bus.PublishAsync("saga.payments.received", new SagaPaymentReceived
{
    PaymentId = "PAY-001"
}, correlationId: sagaCorrelationId);
logger.LogInformation("Saga: Published PaymentReceived for saga {CorrelationId}", sagaCorrelationId);

// --- Comprehensive Fulfillment Saga Demo ---
logger.LogInformation("=== Order Fulfillment Saga Demo ===");

// Happy path: order placed → payment → inventory → shipped → completed
var happyId = Guid.NewGuid().ToString("N");
await bus.PublishAsync("fulfill.order.placed", new FulfillOrderPlaced
{
    OrderId = "FULFILL-001", Amount = 149.99m, CustomerEmail = "happy@example.com"
}, correlationId: happyId);
logger.LogInformation("Fulfillment: Published OrderPlaced (happy path) {Id}", happyId);

await Task.Delay(1500);
await bus.PublishAsync("fulfill.payment.processed", new FulfillPaymentProcessed
{
    PaymentId = "PAY-F001", TransactionRef = "TXN-ABC"
}, correlationId: happyId);

await Task.Delay(1000);
await bus.PublishAsync("fulfill.inventory.reserved", new FulfillInventoryReserved
{
    ReservationId = "RES-001", WarehouseCode = "WH-EAST"
}, correlationId: happyId);

await Task.Delay(1000);
await bus.PublishAsync("fulfill.shipment.dispatched", new FulfillShipmentDispatched
{
    TrackingNumber = "TRACK-12345", Carrier = "FedEx"
}, correlationId: happyId);
logger.LogInformation("Fulfillment: Happy path complete for {Id}", happyId);

// Compensation path: inventory unavailable → refund
var compensateId = Guid.NewGuid().ToString("N");
await bus.PublishAsync("fulfill.order.placed", new FulfillOrderPlaced
{
    OrderId = "FULFILL-002", Amount = 75.00m, CustomerEmail = "compensate@example.com"
}, correlationId: compensateId);

await Task.Delay(1500);
await bus.PublishAsync("fulfill.payment.processed", new FulfillPaymentProcessed
{
    PaymentId = "PAY-F002", TransactionRef = "TXN-DEF"
}, correlationId: compensateId);

await Task.Delay(1000);
await bus.PublishAsync("fulfill.inventory.unavailable", new FulfillInventoryUnavailable
{
    Reason = "Out of stock for SKU-789"
}, correlationId: compensateId);
logger.LogInformation("Fulfillment: Compensation path triggered for {Id}", compensateId);

// Cancel path: cancel from payment processing
var cancelId = Guid.NewGuid().ToString("N");
await bus.PublishAsync("fulfill.order.placed", new FulfillOrderPlaced
{
    OrderId = "FULFILL-003", Amount = 200.00m, CustomerEmail = "cancel@example.com"
}, correlationId: cancelId);

await Task.Delay(1500);
await bus.PublishAsync("fulfill.order.cancelled", new FulfillOrderCancelled
{
    Reason = "Customer changed their mind"
}, correlationId: cancelId);
logger.LogInformation("Fulfillment: Cancel path triggered for {Id}", cancelId);

// High-value order path (>$500, conditional logic)
var highValueId = Guid.NewGuid().ToString("N");
await bus.PublishAsync("fulfill.order.placed", new FulfillOrderPlaced
{
    OrderId = "FULFILL-004", Amount = 1250.00m, CustomerEmail = "highvalue@example.com"
}, correlationId: highValueId);
logger.LogInformation("Fulfillment: High-value order placed for {Id} ($1,250)", highValueId);

logger.LogInformation("Messages published. Waiting for processing... (Press Ctrl+C to exit)");

// Keep the application running to allow the background worker to process messages
await app.WaitForShutdownAsync();

static async Task<bool> SupportsTransactionsAsync(IMongoClient client)
{
    var admin = client.GetDatabase("admin");
    var hello = await admin.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1));
    return hello.Contains("setName");
}

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

// --- Saga Models ---

public record SagaOrderSubmitted
{
    public string OrderId { get; init; } = "";
    public decimal Amount { get; init; }
}

public record SagaPaymentReceived
{
    public string PaymentId { get; init; } = "";
}

public record SagaOrderConfirmation
{
    public string OrderId { get; init; } = "";
    public string Status { get; init; } = "";
}

public class SampleOrderSagaState : ISagaInstance
{
    public string CorrelationId { get; set; } = default!;
    public string CurrentState { get; set; } = default!;
    public int Version { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime LastModifiedUtc { get; set; }

    public string? OrderId { get; set; }
    public decimal Amount { get; set; }
    public bool PaymentReceived { get; set; }
}

public class SampleOrderSagaStateMachine : MongoBusStateMachine<SampleOrderSagaState>
{
    public SagaState Submitted { get; private set; }
    public SagaState PaymentPending { get; private set; }
    public SagaState Completed { get; private set; }

    public SagaEvent<SagaOrderSubmitted> OrderSubmittedEvent { get; private set; }
    public SagaEvent<SagaPaymentReceived> PaymentReceivedEvent { get; private set; }

    public SampleOrderSagaStateMachine()
    {
        Event(() => OrderSubmittedEvent, "saga.orders.submitted", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));
        Event(() => PaymentReceivedEvent, "saga.payments.received", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        InstanceState(x => x.CurrentState);

        Initially(
            When(OrderSubmittedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.OrderId = ctx.Message.OrderId;
                    ctx.Saga.Amount = ctx.Message.Amount;
                })
                .TransitionTo(PaymentPending));

        During(PaymentPending,
            When(PaymentReceivedEvent)
                .Then(ctx => ctx.Saga.PaymentReceived = true)
                .Publish("saga.orders.confirmed", ctx => new SagaOrderConfirmation
                {
                    OrderId = ctx.Saga.OrderId!,
                    Status = "Confirmed"
                })
                .TransitionTo(Completed)
                .Finalize());

        SetCompletedWhenFinalized();
    }
}

// =============================================================================
// Order Fulfillment Saga — Comprehensive Example
// Demonstrates: conditional logic, compensation, exception handling, scheduling,
// DuringAny cancellation, publish, idempotency, history, and partitioning.
// =============================================================================

// --- Fulfillment Messages ---

public record FulfillOrderPlaced
{
    public string OrderId { get; init; } = "";
    public decimal Amount { get; init; }
    public string CustomerEmail { get; init; } = "";
}

public record FulfillPaymentProcessed
{
    public string PaymentId { get; init; } = "";
    public string TransactionRef { get; init; } = "";
}

public record FulfillPaymentFailed
{
    public string Reason { get; init; } = "";
}

public record FulfillInventoryReserved
{
    public string ReservationId { get; init; } = "";
    public string WarehouseCode { get; init; } = "";
}

public record FulfillInventoryUnavailable
{
    public string Reason { get; init; } = "";
}

public record FulfillShipmentDispatched
{
    public string TrackingNumber { get; init; } = "";
    public string Carrier { get; init; } = "";
}

public record FulfillOrderCancelled
{
    public string Reason { get; init; } = "";
}

public record FulfillPaymentTimeoutMsg
{
    public string Message { get; init; } = "Payment timed out";
}

// Compensation / notification messages published by the saga
public record FulfillRefundRequested
{
    public string PaymentId { get; init; } = "";
    public decimal Amount { get; init; }
    public string Reason { get; init; } = "";
}

public record FulfillOrderNotification
{
    public string OrderId { get; init; } = "";
    public string CustomerEmail { get; init; } = "";
    public string Status { get; init; } = "";
    public string? Details { get; init; }
}

// --- Fulfillment State ---

public class OrderFulfillmentState : ISagaInstance
{
    public string CorrelationId { get; set; } = default!;
    public string CurrentState { get; set; } = default!;
    public int Version { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime LastModifiedUtc { get; set; }

    // Order data
    public string? OrderId { get; set; }
    public decimal Amount { get; set; }
    public string? CustomerEmail { get; set; }
    public bool IsHighValue { get; set; }

    // Payment
    public string? PaymentId { get; set; }
    public string? TransactionRef { get; set; }
    public string? PaymentTimeoutToken { get; set; }

    // Inventory
    public string? ReservationId { get; set; }
    public string? WarehouseCode { get; set; }

    // Shipping
    public string? TrackingNumber { get; set; }
    public string? Carrier { get; set; }

    // Error tracking
    public string? FailureReason { get; set; }
    public DateTime? CancelledAt { get; set; }
}

// --- Fulfillment State Machine ---

public class OrderFulfillmentStateMachine : MongoBusStateMachine<OrderFulfillmentState>
{
    // States
    public SagaState PaymentProcessing { get; private set; }
    public SagaState AwaitingApproval { get; private set; }
    public SagaState AwaitingInventory { get; private set; }
    public SagaState Shipping { get; private set; }
    public SagaState Completed { get; private set; }
    public SagaState Compensating { get; private set; }
    public SagaState Faulted { get; private set; }

    // Events
    public SagaEvent<FulfillOrderPlaced> OrderPlacedEvent { get; private set; }
    public SagaEvent<FulfillPaymentProcessed> PaymentProcessedEvent { get; private set; }
    public SagaEvent<FulfillPaymentFailed> PaymentFailedEvent { get; private set; }
    public SagaEvent<FulfillInventoryReserved> InventoryReservedEvent { get; private set; }
    public SagaEvent<FulfillInventoryUnavailable> InventoryUnavailableEvent { get; private set; }
    public SagaEvent<FulfillShipmentDispatched> ShipmentDispatchedEvent { get; private set; }
    public SagaEvent<FulfillOrderCancelled> OrderCancelledEvent { get; private set; }
    public SagaEvent<FulfillPaymentTimeoutMsg> PaymentTimeoutEvent { get; private set; }

    public OrderFulfillmentStateMachine()
    {
        // --- Event declarations ---
        Event(() => OrderPlacedEvent, "fulfill.order.placed", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => PaymentProcessedEvent, "fulfill.payment.processed", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => PaymentFailedEvent, "fulfill.payment.failed", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => InventoryReservedEvent, "fulfill.inventory.reserved", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => InventoryUnavailableEvent, "fulfill.inventory.unavailable", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => ShipmentDispatchedEvent, "fulfill.shipment.dispatched", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        Event(() => OrderCancelledEvent, "fulfill.order.cancelled", e =>
        {
            e.CorrelateById(ctx => ctx.CorrelationId!);
            e.OnMissingInstance(cfg => cfg.Discard()); // Ignore cancel for non-existent sagas
        });

        Event(() => PaymentTimeoutEvent, "fulfill.payment.timeout", e =>
            e.CorrelateById(ctx => ctx.CorrelationId!));

        InstanceState(x => x.CurrentState);

        // --- Initially: order placed ---
        Initially(
            When(OrderPlacedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.OrderId = ctx.Message.OrderId;
                    ctx.Saga.Amount = ctx.Message.Amount;
                    ctx.Saga.CustomerEmail = ctx.Message.CustomerEmail;
                    ctx.Saga.IsHighValue = ctx.Message.Amount > 500m;
                })
                // Conditional: high-value orders need approval, others go straight to payment
                .IfElse(
                    ctx => ctx.Saga.IsHighValue,
                    then => then
                        .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                        {
                            OrderId = ctx.Saga.OrderId!,
                            CustomerEmail = ctx.Saga.CustomerEmail!,
                            Status = "AwaitingApproval",
                            Details = $"High-value order (${ctx.Saga.Amount}) requires manual approval"
                        })
                        .TransitionTo(AwaitingApproval),
                    @else => @else
                        .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                        {
                            OrderId = ctx.Saga.OrderId!,
                            CustomerEmail = ctx.Saga.CustomerEmail!,
                            Status = "PaymentProcessing",
                            Details = "Awaiting payment confirmation"
                        })
                        .TransitionTo(PaymentProcessing)));

        // --- AwaitingApproval: for high-value orders, any payment event moves to PaymentProcessing ---
        During(AwaitingApproval,
            When(PaymentProcessedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.PaymentId = ctx.Message.PaymentId;
                    ctx.Saga.TransactionRef = ctx.Message.TransactionRef;
                })
                .TransitionTo(AwaitingInventory));

        // --- PaymentProcessing ---
        During(PaymentProcessing,
            When(PaymentProcessedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.PaymentId = ctx.Message.PaymentId;
                    ctx.Saga.TransactionRef = ctx.Message.TransactionRef;
                })
                // Clear timeout token (timeout message may still arrive but will be ignored in new state)
                .Then(ctx => ctx.Saga.PaymentTimeoutToken = null)
                .TransitionTo(AwaitingInventory),

            When(PaymentFailedEvent)
                .Then(ctx => ctx.Saga.FailureReason = ctx.Message.Reason)
                .Then(ctx => ctx.Saga.PaymentTimeoutToken = null)
                .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                {
                    OrderId = ctx.Saga.OrderId!,
                    CustomerEmail = ctx.Saga.CustomerEmail!,
                    Status = "PaymentFailed",
                    Details = ctx.Message.Reason
                })
                .TransitionTo(Faulted),

            When(PaymentTimeoutEvent)
                .Then(ctx => ctx.Saga.FailureReason = "Payment timed out")
                .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                {
                    OrderId = ctx.Saga.OrderId!,
                    CustomerEmail = ctx.Saga.CustomerEmail!,
                    Status = "PaymentTimedOut",
                    Details = "Payment was not received within the allowed time"
                })
                .TransitionTo(Faulted));

        // --- AwaitingInventory ---
        During(AwaitingInventory,
            When(InventoryReservedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.ReservationId = ctx.Message.ReservationId;
                    ctx.Saga.WarehouseCode = ctx.Message.WarehouseCode;
                })
                .TransitionTo(Shipping),

            // Compensation: inventory unavailable → refund payment
            When(InventoryUnavailableEvent)
                .Then(ctx => ctx.Saga.FailureReason = ctx.Message.Reason)
                .Publish("fulfill.refund.requested", ctx => new FulfillRefundRequested
                {
                    PaymentId = ctx.Saga.PaymentId!,
                    Amount = ctx.Saga.Amount,
                    Reason = $"Inventory unavailable: {ctx.Message.Reason}"
                })
                .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                {
                    OrderId = ctx.Saga.OrderId!,
                    CustomerEmail = ctx.Saga.CustomerEmail!,
                    Status = "Refunding",
                    Details = $"Inventory unavailable ({ctx.Message.Reason}), initiating refund"
                })
                .TransitionTo(Compensating));

        // --- Shipping ---
        During(Shipping,
            When(ShipmentDispatchedEvent)
                .Then(ctx =>
                {
                    ctx.Saga.TrackingNumber = ctx.Message.TrackingNumber;
                    ctx.Saga.Carrier = ctx.Message.Carrier;
                })
                .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                {
                    OrderId = ctx.Saga.OrderId!,
                    CustomerEmail = ctx.Saga.CustomerEmail!,
                    Status = "Shipped",
                    Details = $"Shipped via {ctx.Message.Carrier}, tracking: {ctx.Message.TrackingNumber}"
                })
                .TransitionTo(Completed)
                .Finalize());

        // --- DuringAny: cancellation from any state ---
        DuringAny(
            When(OrderCancelledEvent)
                .Then(ctx =>
                {
                    ctx.Saga.FailureReason = ctx.Message.Reason;
                    ctx.Saga.CancelledAt = DateTime.UtcNow;
                })
                // If payment was already processed, issue a refund
                .If(ctx => ctx.Saga.PaymentId != null,
                    then => then.Publish("fulfill.refund.requested", ctx => new FulfillRefundRequested
                    {
                        PaymentId = ctx.Saga.PaymentId!,
                        Amount = ctx.Saga.Amount,
                        Reason = $"Order cancelled: {ctx.Message.Reason}"
                    }))
                .Publish("fulfill.notifications", ctx => new FulfillOrderNotification
                {
                    OrderId = ctx.Saga.OrderId!,
                    CustomerEmail = ctx.Saga.CustomerEmail!,
                    Status = "Cancelled",
                    Details = ctx.Message.Reason
                })
                .Finalize());

        // Ignore duplicate placement events in any non-initial state
        DuringAny(
            Ignore(OrderPlacedEvent));

        SetCompletedWhenFinalized();
    }
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
