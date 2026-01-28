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
});

// 2. Configure Dashboard
builder.Services.AddMongoBusDashboard();

// 3. Register Consumers
builder.Services.AddMongoBusConsumer<OrderCreatedHandler, OrderCreated, OrderCreatedDefinition>();
builder.Services.AddMongoBusConsumer<EmailNotificationHandler, EmailNotification, EmailNotificationDefinition>();

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
});

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

// --- Definitions ---

public class OrderCreatedDefinition : ConsumerDefinition<OrderCreatedHandler, OrderCreated>
{
    public override string TypeId => "orders.created";
    public override string EndpointName => "order-processing-service";
}

public class EmailNotificationDefinition : ConsumerDefinition<EmailNotificationHandler, EmailNotification>
{
    public override string TypeId => "notifications.email";
    // Using default EndpointName (will be email-notification)
}
