using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaSharedMessageTypeTests(MongoDbFixture fixture)
{
    private const string InvoicePaidTypeId = "saga.test.shared.invoice-paid";
    private const string ReceiptRequestedTypeId = "saga.test.shared.receipt-requested";
    private const string PaymentAuditedTypeId = "saga.test.shared.payment-audited";

    // --- Messages ---
    public sealed class PaymentReceived
    {
        public string Reference { get; set; } = "";
    }

    // --- Saga States ---
    public sealed class InvoiceState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public sealed class ReceiptState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    // --- State Machines ---
    public class InvoiceStateMachine : MongoBusStateMachine<InvoiceState>
    {
        public SagaState Paid { get; private set; }

        public SagaEvent<PaymentReceived> InvoicePaidEvent { get; private set; }

        public InvoiceStateMachine()
        {
            Event(() => InvoicePaidEvent, InvoicePaidTypeId, e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(InvoicePaidEvent)
                    .TransitionTo(Paid));
        }
    }

    public class ReceiptStateMachine : MongoBusStateMachine<ReceiptState>
    {
        public SagaState Issued { get; private set; }

        public SagaEvent<PaymentReceived> ReceiptRequestedEvent { get; private set; }

        public ReceiptStateMachine()
        {
            Event(() => ReceiptRequestedEvent, ReceiptRequestedTypeId, e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(ReceiptRequestedEvent)
                    .TransitionTo(Issued));
        }
    }

    // --- Plain consumer of the same message type ---
    public sealed class PaymentAuditHandler : IMessageHandler<PaymentReceived>
    {
        public static readonly ConcurrentBag<string> AuditedReferences = [];

        public Task HandleAsync(PaymentReceived message, ConsumeContext context, CancellationToken ct)
        {
            AuditedReferences.Add(message.Reference);
            return Task.CompletedTask;
        }
    }

    public sealed class PaymentAuditDefinition : ConsumerDefinition<PaymentAuditHandler, PaymentReceived>
    {
        public override string TypeId => PaymentAuditedTypeId;
    }

    [Fact]
    public async Task Sagas_Sharing_A_Message_Type_Should_Each_Handle_Their_Own_Events()
    {
        var (sp, bus, db) = Build(services =>
        {
            services.AddMongoBusSaga<InvoiceStateMachine, InvoiceState>();
            services.AddMongoBusSaga<ReceiptStateMachine, ReceiptState>();
        });
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, expectedCount: 2);
            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync(InvoicePaidTypeId, new PaymentReceived { Reference = "INV-1" },
                correlationId: correlationId);

            (await WaitForSagaAsync<InvoiceState>(db, correlationId)).Should()
                .NotBeNull("the invoice saga should handle the invoice event published to its endpoint");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    [Fact]
    public async Task Saga_Should_Handle_Its_Events_When_A_Consumer_Of_The_Same_Message_Type_Is_Registered_After_It()
    {
        var (sp, bus, db) = Build(services =>
        {
            services.AddMongoBusSaga<InvoiceStateMachine, InvoiceState>();
            services.AddMongoBusConsumer<PaymentAuditHandler, PaymentReceived, PaymentAuditDefinition>();
        });
        var hosted = await StartAsync(sp);

        try
        {
            await WaitForBindingsAsync(db, expectedCount: 2);
            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync(InvoicePaidTypeId, new PaymentReceived { Reference = "INV-2" },
                correlationId: correlationId);

            (await WaitForSagaAsync<InvoiceState>(db, correlationId)).Should()
                .NotBeNull("the invoice saga, not the payment audit consumer, should handle the invoice event");
        }
        finally
        {
            await StopAsync(hosted);
        }
    }

    private (ServiceProvider sp, IMessageBus bus, IMongoDatabase db) Build(Action<IServiceCollection> registerHandlers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "saga_shared_type_" + Guid.NewGuid().ToString("N");
        });
        registerHandlers(services);

        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredService<IMessageBus>(), sp.GetRequiredService<IMongoDatabase>());
    }

    private static async Task<List<IHostedService>> StartAsync(ServiceProvider sp)
    {
        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopAsync(IEnumerable<IHostedService> services)
    {
        foreach (var hs in services) await hs.StopAsync(CancellationToken.None);
    }

    private static async Task WaitForBindingsAsync(IMongoDatabase db, int expectedCount)
    {
        var bindings = db.GetCollection<Binding>("bus_bindings");
        var timeout = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < timeout &&
               await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < expectedCount)
        {
            await Task.Delay(100);
        }
    }

    private static async Task<TInstance?> WaitForSagaAsync<TInstance>(IMongoDatabase db, string correlationId)
        where TInstance : class, ISagaInstance
    {
        var collection = db.GetCollection<TInstance>("bus_saga_" + EndpointNameHelper.FromConsumerType(typeof(TInstance)));
        var filter = Builders<TInstance>.Filter.Eq(nameof(ISagaInstance.CorrelationId), correlationId);
        var timeout = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < timeout)
        {
            var instance = await collection.Find(filter).FirstOrDefaultAsync();
            if (instance is not null)
                return instance;

            await Task.Delay(100);
        }

        return null;
    }
}
