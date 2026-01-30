using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class ConfigValidationTests
{
    public sealed class InvalidBatchMessage
    {
    }

    public sealed class InvalidBatchHandler : IBatchMessageHandler<InvalidBatchMessage>
    {
        public Task HandleBatchAsync(IReadOnlyList<InvalidBatchMessage> messages, BatchConsumeContext context, CancellationToken ct) =>
            Task.CompletedTask;
    }

    public sealed class InvalidBatchDefinition : BatchConsumerDefinition<InvalidBatchHandler, InvalidBatchMessage>
    {
        public override string TypeId => "invalid.batch";
        public override BatchConsumerOptions BatchOptions => new()
        {
            MinBatchSize = 1,
            MaxBatchSize = 10,
            MaxBatchWaitTime = TimeSpan.Zero,
            MaxBatchIdleTime = TimeSpan.Zero,
            FlushMode = BatchFlushMode.SinceFirstMessage
        };
    }

    [Fact]
    public async Task Startup_ShouldFail_OnInvalidMongoBusOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = "";
            opt.DatabaseName = "test_invalid";
        });

        var sp = services.BuildServiceProvider();
        var validatorType = typeof(MongoBusOptions).Assembly.GetType("MongoBus.Internal.MongoBusValidationHostedService");
        validatorType.Should().NotBeNull();
        var validator = (IHostedService)sp.GetRequiredService(validatorType!);

        Func<Task> act = async () => await validator.StartAsync(CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*ConnectionString*");
    }

    [Fact]
    public async Task Startup_ShouldFail_OnInvalidBatchOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = "mongodb://localhost:27017";
            opt.DatabaseName = "test_invalid_batch";
        });
        services.AddMongoBusBatchConsumer<InvalidBatchHandler, InvalidBatchMessage, InvalidBatchDefinition>();

        var sp = services.BuildServiceProvider();
        var validatorType = typeof(MongoBusOptions).Assembly.GetType("MongoBus.Internal.MongoBusValidationHostedService");
        validatorType.Should().NotBeNull();
        var validator = (IHostedService)sp.GetRequiredService(validatorType!);

        Func<Task> act = async () => await validator.StartAsync(CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }
}
