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
public class MiddlewareTests(MongoDbFixture fixture)
{
    public record InterceptorMessage(string Text);

    public class InterceptorHandler : IMessageHandler<InterceptorMessage>
    {
        public static int CallCount;
        public Task HandleAsync(InterceptorMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            return Task.CompletedTask;
        }
    }

    public class InterceptorDefinition : ConsumerDefinition<InterceptorHandler, InterceptorMessage>
    {
        public override string TypeId => "interceptor.message";
    }

    public class TestPublishInterceptor : IPublishInterceptor
    {
        public static int CallCount;
        public Task OnPublishAsync<T>(PublishContext<T> context, Func<Task> next, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            return next();
        }
    }

    public class TestConsumeInterceptor : IConsumeInterceptor
    {
        public static int CallCount;
        public Task OnConsumeAsync(ConsumeContext context, object message, Func<Task> next, CancellationToken ct)
        {
            Interlocked.Increment(ref CallCount);
            return next();
        }
    }

    [Fact]
    public async Task Interceptors_ShouldBeCalled()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "middleware_test_" + Guid.NewGuid().ToString("N");
        });
        
        services.AddMongoBusConsumer<InterceptorHandler, InterceptorMessage, InterceptorDefinition>();
        services.AddMongoBusPublishInterceptor<TestPublishInterceptor>();
        services.AddMongoBusConsumeInterceptor<TestConsumeInterceptor>();

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        foreach (var hs in hostedServices) await hs.StartAsync(CancellationToken.None);

        try
        {
            InterceptorHandler.CallCount = 0;
            TestPublishInterceptor.CallCount = 0;
            TestConsumeInterceptor.CallCount = 0;

            // Wait for bindings
            var bindings = db.GetCollection<Binding>("bus_bindings");
            var timeout = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < timeout && await bindings.CountDocumentsAsync(FilterDefinition<Binding>.Empty) == 0)
            {
                await Task.Delay(100);
            }

            // Act
            await bus.PublishAsync("interceptor.message", new InterceptorMessage("Hello"));

            // Assert
            var waitTimeout = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < waitTimeout && InterceptorHandler.CallCount == 0)
            {
                await Task.Delay(100);
            }

            TestPublishInterceptor.CallCount.Should().Be(1, "Publish interceptor should be called once");
            TestConsumeInterceptor.CallCount.Should().Be(1, "Consume interceptor should be called once");
            InterceptorHandler.CallCount.Should().Be(1, "Handler should be called once");
        }
        finally
        {
            foreach (var hs in hostedServices) await hs.StopAsync(CancellationToken.None);
        }
    }
}
