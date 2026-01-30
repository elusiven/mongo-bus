using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class BatchConsumerTests(MongoDbFixture fixture)
{
    public sealed record BatchMessage(int Index);

    public sealed class BatchHandler : IBatchMessageHandler<BatchMessage>
    {
        public static readonly ConcurrentQueue<int> BatchSizes = new();
        public static int TotalMessages;

        public Task HandleBatchAsync(IReadOnlyList<BatchMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            BatchSizes.Enqueue(messages.Count);
            Interlocked.Add(ref TotalMessages, messages.Count);
            return Task.CompletedTask;
        }
    }

    public sealed class BatchDefinition : BatchConsumerDefinition<BatchHandler, BatchMessage>
    {
        public override string TypeId => "batch.message";
        public override int ConcurrencyLimit => 1;
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 3,
            MaxBatchWaitTime = TimeSpan.FromSeconds(2),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    public sealed class TimeoutBatchHandler : IBatchMessageHandler<BatchMessage>
    {
        public static readonly ConcurrentQueue<int> BatchSizes = new();

        public Task HandleBatchAsync(IReadOnlyList<BatchMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            BatchSizes.Enqueue(messages.Count);
            return Task.CompletedTask;
        }
    }

    public sealed class TimeoutBatchDefinition : BatchConsumerDefinition<TimeoutBatchHandler, BatchMessage>
    {
        public override string TypeId => "batch.timeout";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 5,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    public sealed class IdleBatchHandler : IBatchMessageHandler<BatchMessage>
    {
        public static readonly ConcurrentQueue<int> BatchSizes = new();

        public Task HandleBatchAsync(IReadOnlyList<BatchMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            BatchSizes.Enqueue(messages.Count);
            return Task.CompletedTask;
        }
    }

    public sealed class IdleBatchDefinition : BatchConsumerDefinition<IdleBatchHandler, BatchMessage>
    {
        public override string TypeId => "batch.idle";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.Zero,
            MaxBatchIdleTime = TimeSpan.FromMilliseconds(300),
            FlushMode = BatchFlushMode.SinceLastMessage
        };
    }

    public sealed record GroupedMessage(string GroupId, int Index);

    public sealed class GroupedBatchHandler : IBatchMessageHandler<GroupedMessage>
    {
        public static readonly ConcurrentDictionary<string, int> GroupCounts = new();

        public Task HandleBatchAsync(IReadOnlyList<GroupedMessage> messages, BatchConsumeContext context, CancellationToken ct)
        {
            var key = context.GroupKey ?? "__none__";
            GroupCounts.AddOrUpdate(key, messages.Count, (_, existing) => existing + messages.Count);
            return Task.CompletedTask;
        }
    }

    public sealed class GroupedBatchDefinition : BatchConsumerDefinition<GroupedBatchHandler, GroupedMessage>
    {
        public override string TypeId => "batch.grouped";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.FromSeconds(1),
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };

        public override IBatchGroupingStrategy GroupingStrategy =>
            BatchGrouping.ByMessage<GroupedMessage>(m => m.GroupId);
    }

    [Fact]
    public async Task ShouldProcessInBatchesWithMaxSize()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "batch_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusBatchConsumer<BatchHandler, BatchMessage, BatchDefinition>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            while (BatchHandler.BatchSizes.TryDequeue(out _)) { }
            BatchHandler.TotalMessages = 0;

            for (var i = 0; i < 5; i++)
            {
                await bus.PublishAsync("batch.message", new BatchMessage(i), "test-source");
            }

            var timeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < timeout && BatchHandler.TotalMessages < 5)
            {
                await Task.Delay(100);
            }

            BatchHandler.TotalMessages.Should().Be(5);
            var sizes = BatchHandler.BatchSizes.ToArray();
            sizes.Should().Contain(3);
            sizes.Should().Contain(2);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ShouldFlushBatchOnMaxWaitTime()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "batch_timeout_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusBatchConsumer<TimeoutBatchHandler, BatchMessage, TimeoutBatchDefinition>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            while (TimeoutBatchHandler.BatchSizes.TryDequeue(out _)) { }

            await bus.PublishAsync("batch.timeout", new BatchMessage(1), "test-source");
            await bus.PublishAsync("batch.timeout", new BatchMessage(2), "test-source");

            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && TimeoutBatchHandler.BatchSizes.IsEmpty)
            {
                await Task.Delay(100);
            }

            var sizes = TimeoutBatchHandler.BatchSizes.ToArray();
            sizes.Should().ContainSingle();
            sizes[0].Should().Be(2);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ShouldFlushBatchOnIdleTimeSinceLastMessage()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "batch_idle_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusBatchConsumer<IdleBatchHandler, BatchMessage, IdleBatchDefinition>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            while (IdleBatchHandler.BatchSizes.TryDequeue(out _)) { }

            await bus.PublishAsync("batch.idle", new BatchMessage(1), "test-source");
            await bus.PublishAsync("batch.idle", new BatchMessage(2), "test-source");

            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && IdleBatchHandler.BatchSizes.IsEmpty)
            {
                await Task.Delay(100);
            }

            var sizes = IdleBatchHandler.BatchSizes.ToArray();
            sizes.Should().ContainSingle();
            sizes[0].Should().Be(2);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ShouldGroupBatchByMessageProperty()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "batch_grouped_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusBatchConsumer<GroupedBatchHandler, GroupedMessage, GroupedBatchDefinition>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var bindingTimeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < bindingTimeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            GroupedBatchHandler.GroupCounts.Clear();

            await bus.PublishAsync("batch.grouped", new GroupedMessage("A", 1), "test-source");
            await bus.PublishAsync("batch.grouped", new GroupedMessage("B", 2), "test-source");
            await bus.PublishAsync("batch.grouped", new GroupedMessage("A", 3), "test-source");

            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && GroupedBatchHandler.GroupCounts.Count < 2)
            {
                await Task.Delay(100);
            }

            GroupedBatchHandler.GroupCounts.Should().ContainKey("A");
            GroupedBatchHandler.GroupCounts.Should().ContainKey("B");
            GroupedBatchHandler.GroupCounts["A"].Should().Be(2);
            GroupedBatchHandler.GroupCounts["B"].Should().Be(1);
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
