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
public class IdempotencyRaceTests(MongoDbFixture fixture)
{
    private const string RacingTypeId = "idempotency.race.message";
    private const int CopiesPublished = 2;
    private static readonly TimeSpan HandlerDuration = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(25);

    public sealed class RacingMessage { }

    /// <summary>
    /// Slow enough that both copies are being handled at once: the existing check only skips a copy once another
    /// has already been recorded as processed, so a handler that returns immediately hides the race.
    /// </summary>
    public sealed class RacingHandler : IMessageHandler<RacingMessage>
    {
        public static int HandleCount;

        public async Task HandleAsync(RacingMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref HandleCount);
            await Task.Delay(HandlerDuration, ct);
        }
    }

    public sealed class RacingDefinition : ConsumerDefinition<RacingHandler, RacingMessage>
    {
        public override string TypeId => RacingTypeId;
        public override int ConcurrencyLimit => 5;
        public override bool IdempotencyEnabled => true;
    }

    [Fact]
    public async Task ShouldHandleACloudEventOnceWhenItsCopiesAreConsumedAtTheSameTime()
    {
        Interlocked.Exchange(ref RacingHandler.HandleCount, 0);

        await using var bus = await RunningBus.StartAsync(
            fixture.ConnectionString,
            registerServices: services =>
                services.AddMongoBusConsumer<RacingHandler, RacingMessage, RacingDefinition>());

        var messageBus = bus.Services.GetRequiredService<IMessageBus>();
        var cloudEventId = "race-" + Guid.NewGuid().ToString("N");

        for (var i = 0; i < CopiesPublished; i++)
            await messageBus.PublishAsync(RacingTypeId, new RacingMessage(), id: cloudEventId);

        await WaitUntilBothCopiesSettleAsync(bus.Database);

        RacingHandler.HandleCount.Should().Be(1,
            "both copies carry the same CloudEvent id, so whichever consumer claims it first should be the only "
            + "one to handle it, even when the other is consumed before the first has finished");
    }

    private static async Task WaitUntilBothCopiesSettleAsync(IMongoDatabase db)
    {
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadline = DateTime.UtcNow.Add(SettleTimeout);

        while (DateTime.UtcNow < deadline)
        {
            var settled = await inbox.CountDocumentsAsync(
                x => x.TypeId == RacingTypeId && x.Status == "Processed");

            if (settled >= CopiesPublished)
                return;

            await Task.Delay(100);
        }

        throw new TimeoutException($"Both '{RacingTypeId}' copies were not settled within {SettleTimeout}.");
    }
}
