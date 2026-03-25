using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Models.Saga;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests.Saga;

[Collection("Mongo collection")]
public class SagaTimeoutTests(MongoDbFixture fixture)
{
    public sealed class TimeoutStartMsg
    {
        public string Id { get; set; } = "";
    }

    public sealed class TimeoutTestState : ISagaInstance
    {
        public string CorrelationId { get; set; } = default!;
        public string CurrentState { get; set; } = default!;
        public int Version { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public class TimeoutStateMachine : MongoBusStateMachine<TimeoutTestState>
    {
        public SagaState Processing { get; private set; }

        public SagaEvent<TimeoutStartMsg> StartEvent { get; private set; }

        public TimeoutStateMachine()
        {
            Event(() => StartEvent, "saga.test.timeout.start", e =>
                e.CorrelateById(ctx => ctx.CorrelationId!));

            InstanceState(x => x.CurrentState);

            Initially(
                When(StartEvent).TransitionTo(Processing));
        }
    }

    [Fact]
    public async Task SagaTimeout_TransitionsExpiredInstance()
    {
        var dbName = "saga_timeout_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<TimeoutStateMachine, TimeoutTestState>(opt =>
        {
            opt.SagaTimeout = TimeSpan.FromSeconds(2);
            opt.TimeoutScanInterval = TimeSpan.FromSeconds(1);
        });

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout &&
                   await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < 1)
                await Task.Delay(100);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.timeout.start",
                new TimeoutStartMsg { Id = "T-1" },
                correlationId: correlationId);

            // Wait for Processing state
            var collection = db.GetCollection<TimeoutTestState>("bus_saga_timeout-test-state");
            var sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            // Wait for the timeout service to transition the saga (2s timeout + 1s scan interval)
            sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "TimedOut") break;
                await Task.Delay(200);
            }

            var state = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("TimedOut");
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SagaTimeout_DoesNotAffectActiveInstance()
    {
        var dbName = "saga_timeout_active_" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = dbName;
        });
        services.AddMongoBusSaga<TimeoutStateMachine, TimeoutTestState>(opt =>
        {
            opt.SagaTimeout = TimeSpan.FromSeconds(30); // Long timeout
            opt.TimeoutScanInterval = TimeSpan.FromSeconds(1);
        });

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hosted = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hosted) await hs.StartAsync(CancellationToken.None);

        try
        {
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout &&
                   await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) < 1)
                await Task.Delay(100);

            var correlationId = Guid.NewGuid().ToString("N");

            await bus.PublishAsync("saga.test.timeout.start",
                new TimeoutStartMsg { Id = "T-2" },
                correlationId: correlationId);

            // Wait for Processing state
            var collection = db.GetCollection<TimeoutTestState>("bus_saga_timeout-test-state");
            var sagaTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < sagaTimeout)
            {
                var s = await collection.Find(x => x.CorrelationId == correlationId).FirstOrDefaultAsync();
                if (s?.CurrentState == "Processing") break;
                await Task.Delay(100);
            }

            // Wait a couple scan intervals to confirm no timeout
            await Task.Delay(3000);

            var state = await collection
                .Find(x => x.CorrelationId == correlationId)
                .FirstOrDefaultAsync();

            state.Should().NotBeNull();
            state!.CurrentState.Should().Be("Processing");
        }
        finally
        {
            foreach (var hs in hosted) await hs.StopAsync(CancellationToken.None);
        }
    }
}
