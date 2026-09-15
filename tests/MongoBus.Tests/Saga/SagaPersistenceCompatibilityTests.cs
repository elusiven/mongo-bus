using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Internal.Saga;
using MongoBus.Models.Saga;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaPersistenceCompatibilityTests(MongoDbFixture fixture)
{
    private const string SagaCollectionName = "bus_saga_legacy-invoice-state";

    public sealed class InvoiceRaised
    {
        public decimal Amount { get; set; }
    }

    public sealed class LegacyInvoiceState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
        public decimal Amount { get; set; }
    }

    public class LegacyInvoiceStateMachine : MongoBusStateMachine<LegacyInvoiceState>
    {
        public SagaState Raised { get; private set; }
        public SagaEvent<InvoiceRaised> InvoiceRaisedEvent { get; private set; }

        public LegacyInvoiceStateMachine()
        {
            Event(() => InvoiceRaisedEvent, "saga.compat.invoice.raised", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(InvoiceRaisedEvent)
                    .Then(ctx => ctx.Saga.Amount = ctx.Message.Amount)
                    .TransitionTo(Raised));
        }
    }

    [Fact]
    public async Task Should_Load_Saga_Instance_Whose_Decimal_Was_Stored_As_String_By_Driver_2()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(o =>
        {
            o.ConnectionString = fixture.ConnectionString;
            o.DatabaseName = "saga_compat_decimal_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusSaga<LegacyInvoiceStateMachine, LegacyInvoiceState>();
        var sp = services.BuildServiceProvider();
        var correlationId = Guid.NewGuid().ToString("N");
        await SeedSagaDocumentAsWrittenByDriver2(sp.GetRequiredService<IMongoDatabase>(), correlationId);
        var repository = sp.GetRequiredService<ISagaRepository<LegacyInvoiceState>>();

        var instance = await repository.FindAsync(correlationId, CancellationToken.None);

        instance.Should().NotBeNull();
        instance!.Amount.Should().Be(149.99m);
    }

    private static Task SeedSagaDocumentAsWrittenByDriver2(IMongoDatabase db, string correlationId)
    {
        var now = DateTime.UtcNow;
        var driver2Document = new BsonDocument
        {
            { "CorrelationId", correlationId },
            { "CurrentState", "Raised" },
            { "Version", 1 },
            { "CreatedUtc", now },
            { "LastModifiedUtc", now },
            { "Amount", "149.99" }
        };

        return db.GetCollection<BsonDocument>(SagaCollectionName).InsertOneAsync(driver2Document);
    }
}
