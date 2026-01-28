using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoDB.Driver;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class OpenTelemetryTests(MongoDbFixture fixture)
{
    public sealed class TraceMessage { }

    public sealed class TraceHandler : IMessageHandler<TraceMessage>
    {
        public static Activity? HandlerActivity;
        public Task HandleAsync(TraceMessage message, MongoBus.Models.ConsumeContext context, CancellationToken ct)
        {
            HandlerActivity = Activity.Current;
            return Task.CompletedTask;
        }
    }

    public sealed class TraceDefinition : ConsumerDefinition<TraceHandler, TraceMessage>
    {
        public override string TypeId => "trace.message";
    }

    [Fact]
    public async Task PublishAndConsume_ShouldPropagateTraceContext()
    {
        // Arrange
        var activities = new List<Activity>();
        using var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddSource(MongoBusDiagnostics.ActivitySourceName)
            .AddSource("TestRoot")
            .AddInMemoryExporter(activities)
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "otel_test_" + Guid.NewGuid().ToString("N");
        });
        services.AddMongoBusConsumer<TraceHandler, TraceMessage, TraceDefinition>();

        var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IMessageBus>();
        var db = sp.GetRequiredService<IMongoDatabase>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            TraceHandler.HandlerActivity = null;

            // Wait for binding
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && await bindings.CountDocumentsAsync(x => x.Topic == "trace.message") == 0)
            {
                await Task.Delay(100);
            }

            // Act
            Activity? rootActivity = null;
            using (var source = new ActivitySource("TestRoot"))
            {
                rootActivity = source.StartActivity("RootOperation");
                await bus.PublishAsync("trace.message", new TraceMessage());
                rootActivity?.Stop();
            }

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && TraceHandler.HandlerActivity == null)
            {
                await Task.Delay(100);
            }

            TraceHandler.HandlerActivity.Should().NotBeNull();
            
            // Wait a bit for activities to be exported to in-memory list
            await Task.Delay(500);

            var publishActivity = activities.FirstOrDefault(a => a.OperationName == "trace.message publish");
            var consumeActivity = activities.FirstOrDefault(a => a.OperationName == "trace.message consume");

            publishActivity.Should().NotBeNull();
            consumeActivity.Should().NotBeNull();

            // Trace IDs should match
            publishActivity!.TraceId.Should().Be(rootActivity!.TraceId);
            consumeActivity!.TraceId.Should().Be(rootActivity.TraceId);

            // Hierarchy: Root -> Publish -> Consume
            publishActivity.ParentId.Should().Be(rootActivity.Id);
            consumeActivity.ParentId.Should().Be(publishActivity.Id);
            
            TraceHandler.HandlerActivity!.Id.Should().Be(consumeActivity.Id);
            
            // Verify tags
            publishActivity.TagObjects.Should().Contain(t => t.Key == "messaging.system" && (string?)t.Value == "mongodb");
            consumeActivity.TagObjects.Should().Contain(t => t.Key == "messaging.operation" && (string?)t.Value == "process");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
