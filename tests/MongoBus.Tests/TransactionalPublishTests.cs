using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo replica set collection")]
public class TransactionalPublishTests(MongoReplicaSetFixture fixture)
{
    private const string TypeId = "transactional.publish.message";

    public sealed record TransactionalMessage(string Text);

    [Fact]
    public async Task Publish_With_Transaction_Should_Commit_The_Callbacks_Writes_Together_With_The_Outbox_Message()
    {
        var bus = CreateBus(out var db);
        var orders = db.GetCollection<BsonDocument>("orders");

        await bus.PublishWithTransactionAsync(TypeId, new TransactionalMessage("committed"),
            (session, ct) => orders.InsertOneAsync(session, new BsonDocument("order", 1), cancellationToken: ct));

        (await orders.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(1);
        (await OutboxMessageCountAsync(db)).Should().Be(1);
    }

    [Fact]
    public async Task Publish_With_Transaction_Should_Roll_Back_Everything_When_The_Callback_Fails()
    {
        var bus = CreateBus(out var db);
        var orders = db.GetCollection<BsonDocument>("orders");

        var act = () => bus.PublishWithTransactionAsync(TypeId, new TransactionalMessage("rolled back"),
            async (session, ct) =>
            {
                await orders.InsertOneAsync(session, new BsonDocument("order", 1), cancellationToken: ct);
                throw new InvalidOperationException("callback failed");
            });

        await act.Should().ThrowAsync<InvalidOperationException>();
        (await orders.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(0);
        (await OutboxMessageCountAsync(db)).Should().Be(0);
    }

    [Fact]
    public async Task Publish_With_Transaction_Should_Retry_After_A_Transient_Transaction_Error()
    {
        var applicationName = "transactional-publish-" + Guid.NewGuid().ToString("N");
        var bus = CreateBus(out var db, applicationName);
        var orders = db.GetCollection<BsonDocument>("orders");
        await using var writeConflict = await TransientWriteConflict.InjectOnceAsync(fixture.ConnectionString, applicationName);

        await bus.PublishWithTransactionAsync(TypeId, new TransactionalMessage("retried"),
            (session, ct) => orders.InsertOneAsync(session, new BsonDocument("order", 1), cancellationToken: ct));

        (await orders.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(1,
            "a write conflict labelled TransientTransactionError is resolved by running the transaction again");
        (await OutboxMessageCountAsync(db)).Should().Be(1);
    }

    [Fact]
    public async Task Publish_With_Transaction_Should_Surface_The_Callbacks_Exception_Even_When_Its_Token_Was_Cancelled()
    {
        var bus = CreateBus(out var db);
        var orders = db.GetCollection<BsonDocument>("orders");
        using var cancellation = new CancellationTokenSource();

        var act = () => bus.PublishWithTransactionAsync(TypeId, new TransactionalMessage("cancelled"),
            async (session, ct) =>
            {
                // The write starts the transaction on the server, so aborting it needs a round trip.
                await orders.InsertOneAsync(session, new BsonDocument("order", 1), cancellationToken: ct);
                await cancellation.CancelAsync();
                throw new InvalidOperationException("callback failed");
            },
            ct: cancellation.Token);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("callback failed",
            "cleaning up the transaction must not replace the error that caused it to fail");
    }

    private ITransactionalMessageBus CreateBus(out IMongoDatabase db, string? applicationName = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = new MongoUrlBuilder(fixture.ConnectionString) { ApplicationName = applicationName }.ToString();
            opt.DatabaseName = "transactional_publish_" + Guid.NewGuid().ToString("N");
            opt.Outbox.Enabled = true;
        });

        var provider = services.BuildServiceProvider();
        db = provider.GetRequiredService<IMongoDatabase>();
        return provider.GetRequiredService<ITransactionalMessageBus>();
    }

    private static Task<long> OutboxMessageCountAsync(IMongoDatabase db) =>
        db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName)
            .CountDocumentsAsync(x => x.TypeId == TypeId);

    /// <summary>
    /// Makes MongoDB reject the next insert from one application with a WriteConflict labelled
    /// TransientTransactionError, the error concurrent transactions touching the same documents produce.
    /// </summary>
    private sealed class TransientWriteConflict(IMongoDatabase admin) : IAsyncDisposable
    {
        private const int WriteConflict = 112;

        public static async Task<TransientWriteConflict> InjectOnceAsync(string connectionString, string applicationName)
        {
            var admin = new MongoClient(connectionString).GetDatabase("admin");
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = new BsonDocument("times", 1),
                ["data"] = new BsonDocument
                {
                    ["failCommands"] = new BsonArray { "insert" },
                    ["errorCode"] = WriteConflict,
                    ["errorLabels"] = new BsonArray { "TransientTransactionError" },
                    ["appName"] = applicationName
                }
            });
            return new TransientWriteConflict(admin);
        }

        public async ValueTask DisposeAsync() =>
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = "off"
            });
    }
}
