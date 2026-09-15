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
public class ClaimCheckPayloadLimitTests(MongoDbFixture fixture)
{
    private const string LimitTypeId = "claimcheck.limit.message";
    private const int MaxPayloadBytes = 1024;
    private static readonly TimeSpan OutcomeTimeout = TimeSpan.FromSeconds(15);

    public sealed record LimitTestMessage(string Text);

    public sealed class RecordingHandler : IMessageHandler<LimitTestMessage>
    {
        public static readonly ConcurrentBag<string> ReceivedCloudEventIds = [];

        public Task HandleAsync(LimitTestMessage message, ConsumeContext context, CancellationToken ct)
        {
            ReceivedCloudEventIds.Add(context.CloudEventId);
            return Task.CompletedTask;
        }
    }

    public sealed class SingleAttemptDefinition : ConsumerDefinition<RecordingHandler, LimitTestMessage>
    {
        public override string TypeId => LimitTypeId;
        public override int MaxAttempts => 1;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Payload_Larger_Than_The_Limit_Should_Be_Dead_Lettered_Without_Reaching_The_Handler(bool compressed)
    {
        await using var bus = await StartBusAsync(compressed);
        var cloudEventId = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>()
            .PublishAsync(LimitTypeId, new LimitTestMessage(new string('x', MaxPayloadBytes * 4)), id: cloudEventId);

        var stored = await WaitForOutcomeAsync(bus, cloudEventId);
        stored.Status.Should().Be("Dead", "a consumer must not read more of a claim-check payload than the configured limit");
        stored.LastError.Should().Contain(nameof(ClaimCheckCompressionOptions.MaxDecompressedBytes), "the error should name the setting that limits the payload");
        RecordingHandler.ReceivedCloudEventIds.Should().NotContain(cloudEventId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Payload_Within_The_Limit_Should_Reach_The_Handler(bool compressed)
    {
        await using var bus = await StartBusAsync(compressed);
        var cloudEventId = Guid.NewGuid().ToString("N");

        await bus.Services.GetRequiredService<IMessageBus>()
            .PublishAsync(LimitTypeId, new LimitTestMessage(new string('x', MaxPayloadBytes / 2)), id: cloudEventId);

        var stored = await WaitForOutcomeAsync(bus, cloudEventId);
        stored.Status.Should().Be("Processed");
        RecordingHandler.ReceivedCloudEventIds.Should().Contain(cloudEventId);
    }

    private Task<RunningBus> StartBusAsync(bool compressed) =>
        RunningBus.StartAsync(
            fixture.ConnectionString,
            opt =>
            {
                opt.ClaimCheck.Enabled = true;
                opt.ClaimCheck.ProviderName = "memory";
                opt.ClaimCheck.ThresholdBytes = 1;
                opt.ClaimCheck.Compression.Enabled = compressed;
                opt.ClaimCheck.Compression.MaxDecompressedBytes = MaxPayloadBytes;
            },
            services =>
            {
                services.AddMongoBusInMemoryClaimCheck();
                services.AddMongoBusConsumer<RecordingHandler, LimitTestMessage, SingleAttemptDefinition>();
            });

    private static async Task<InboxMessage> WaitForOutcomeAsync(RunningBus bus, string cloudEventId)
    {
        var inbox = bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadline = DateTime.UtcNow.Add(OutcomeTimeout);
        InboxMessage stored;
        do
        {
            await Task.Delay(100);
            stored = await inbox.Find(x => x.CloudEventId == cloudEventId).SingleAsync();
        } while (stored.Status == "Pending" && DateTime.UtcNow < deadline);

        return stored;
    }
}
