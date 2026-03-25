using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class TransactionalBusEdgeTests(MongoDbFixture fixture)
{
    public sealed class TxEdgeMessage
    {
        public string Text { get; set; } = "";
    }

    public sealed class TxEdgeHandler : IMessageHandler<TxEdgeMessage>
    {
        public Task HandleAsync(TxEdgeMessage message, ConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class TxEdgeDefinition : ConsumerDefinition<TxEdgeHandler, TxEdgeMessage>
    {
        public override string TypeId => "tx.edge.message";
        public override string EndpointName => "tx-edge-endpoint";
    }

    public sealed class TestPublishInterceptor : IPublishInterceptor
    {
        public static int CallCount;

        public async Task OnPublishAsync<T>(PublishContext<T> context, Func<Task> next, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            await next();
        }
    }

    public sealed class TestPublishObserver : IPublishObserver
    {
        public ConcurrentQueue<PublishMetrics> Published { get; } = new();
        public ConcurrentQueue<PublishFailureMetrics> Failed { get; } = new();

        public void OnPublish(PublishMetrics metrics) => Published.Enqueue(metrics);
        public void OnPublishFailed(PublishFailureMetrics metrics) => Failed.Enqueue(metrics);
    }

    [Fact]
    public async Task PublishToOutbox_WhenOutboxDisabled_Throws()
    {
        var services = BuildServices(opt => opt.Outbox.Enabled = false);
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();

        Func<Task> act = () => transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "should-fail" });

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Outbox is disabled*");
    }

    [Fact]
    public async Task PublishToOutbox_InsertsMessageInOutboxCollection()
    {
        var dbName = "tx_edge_insert_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);

        await transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "inserted" });

        var msg = await outbox.Find(x => x.Topic == "tx.edge.message").FirstOrDefaultAsync();
        msg.Should().NotBeNull();
        msg!.Status.Should().Be("Pending");
        msg.PayloadJson.Should().Contain("inserted");
    }

    [Fact]
    public async Task PublishToOutbox_WithInterceptors_CallsInterceptor()
    {
        var dbName = "tx_edge_interceptor_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        services.AddMongoBusPublishInterceptor<TestPublishInterceptor>();
        var sp = services.BuildServiceProvider();

        TestPublishInterceptor.CallCount = 0;

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();

        await transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "intercepted" });

        TestPublishInterceptor.CallCount.Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task PublishToOutbox_NotifiesObservers()
    {
        var dbName = "tx_edge_observer_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        services.AddMongoBusPublishObserver<TestPublishObserver>();
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var observer = sp.GetServices<IPublishObserver>().OfType<TestPublishObserver>().First();

        await transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "observed" });

        observer.Published.Should().NotBeEmpty();
        observer.Published.TryPeek(out var metrics);
        metrics.Should().NotBeNull();
        metrics!.TypeId.Should().Be("tx.edge.message");
    }

    [Fact]
    public async Task PublishToOutbox_OnError_NotifiesObserverFailed()
    {
        var dbName = "tx_edge_fail_observer_" + Guid.NewGuid().ToString("N");

        // Use an invalid connection string to force an error during outbox insert
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = "mongodb://invalid-host-that-does-not-exist:27017";
            opt.DatabaseName = dbName;
            opt.Outbox.Enabled = true;
            opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(50);
        });
        services.AddMongoBusPublishObserver<TestPublishObserver>();
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var observer = sp.GetServices<IPublishObserver>().OfType<TestPublishObserver>().First();

        Func<Task> act = () => transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "will-fail" });

        await act.Should().ThrowAsync<Exception>();

        observer.Failed.Should().NotBeEmpty();
        observer.Failed.TryPeek(out var failMetrics);
        failMetrics.Should().NotBeNull();
        failMetrics!.TypeId.Should().Be("tx.edge.message");
        failMetrics.Exception.Should().NotBeNull();
    }

    [Fact]
    public async Task PublishWithTransaction_NullCallback_Throws()
    {
        var dbName = "tx_edge_null_cb_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();

        Func<Task> act = () => transactionalBus.PublishWithTransactionAsync<TxEdgeMessage>(
            "tx.edge.message",
            new TxEdgeMessage { Text = "null-callback" },
            null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task PublishToOutbox_WithCorrelationAndCausation_StoresInOutbox()
    {
        var dbName = "tx_edge_corr_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);

        var correlationId = Guid.NewGuid().ToString("N");
        var causationId = Guid.NewGuid().ToString("N");

        await transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "correlated" },
            correlationId: correlationId,
            causationId: causationId);

        var msg = await outbox.Find(x => x.Topic == "tx.edge.message").FirstOrDefaultAsync();
        msg.Should().NotBeNull();
        msg!.CorrelationId.Should().Be(correlationId);
        msg.CausationId.Should().Be(causationId);
    }

    [Fact]
    public async Task PublishToOutbox_WithDeliverAt_SetsVisibleUtc()
    {
        var dbName = "tx_edge_deliver_" + Guid.NewGuid().ToString("N");
        var services = BuildServices(opt => opt.DatabaseName = dbName);
        var sp = services.BuildServiceProvider();

        var transactionalBus = sp.GetRequiredService<ITransactionalMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var outbox = db.GetCollection<OutboxMessage>(MongoBusConstants.OutboxCollectionName);

        var deliverAt = DateTime.UtcNow.AddMinutes(30);

        await transactionalBus.PublishToOutboxAsync(
            "tx.edge.message",
            new TxEdgeMessage { Text = "delayed" },
            deliverAt: deliverAt);

        var msg = await outbox.Find(x => x.Topic == "tx.edge.message").FirstOrDefaultAsync();
        msg.Should().NotBeNull();
        msg!.VisibleUtc.Should().BeCloseTo(deliverAt, TimeSpan.FromSeconds(2));
    }

    private ServiceCollection BuildServices(Action<MongoBusOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "tx_edge_" + Guid.NewGuid().ToString("N");
            opt.Outbox.Enabled = true;
            opt.Outbox.PollingInterval = TimeSpan.FromMilliseconds(50);
            opt.Outbox.LockTime = TimeSpan.FromSeconds(10);
            opt.Outbox.MaxAttempts = 3;
            configure?.Invoke(opt);
        });
        return services;
    }
}
