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
public class ConcurrencyTests(MongoDbFixture fixture)
{
    private const string SlowTypeId = "slow.message";
    private static readonly TimeSpan HandlerDuration = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan ProcessedTimeout = TimeSpan.FromSeconds(15);

    public sealed class SlowMessage { }

    public sealed class SlowHandler : IMessageHandler<SlowMessage>
    {
        public static int StartCount;

        public async Task HandleAsync(SlowMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref StartCount);
            await Task.Delay(HandlerDuration, ct);
        }
    }

    public sealed class SlowDefinition : ConsumerDefinition<SlowHandler, SlowMessage>
    {
        public override string TypeId => SlowTypeId;
        public override int ConcurrencyLimit => 5;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
    }

    [Fact]
    public async Task MessageShouldNotBeProcessedByMultipleWorkersSimultaneously()
    {
        var databaseName = "concurrency_test_" + Guid.NewGuid().ToString("N");
        await using var firstInstance = await StartInstanceAsync(databaseName);
        await using var secondInstance = await StartInstanceAsync(databaseName);
        Interlocked.Exchange(ref SlowHandler.StartCount, 0);

        await firstInstance.Services.GetRequiredService<IMessageBus>().PublishAsync(SlowTypeId, new SlowMessage(), "test-source");
        await WaitUntilProcessedAsync(firstInstance.Database);

        SlowHandler.StartCount.Should().Be(1,
            "ten workers across two instances compete for the message, but only the one holding its lock may handle it");
    }

    private Task<RunningBus> StartInstanceAsync(string databaseName) =>
        RunningBus.StartAsync(
            fixture.ConnectionString,
            opt => opt.DatabaseName = databaseName,
            services => services.AddMongoBusConsumer<SlowHandler, SlowMessage, SlowDefinition>());

    private static async Task WaitUntilProcessedAsync(IMongoDatabase db)
    {
        var inbox = db.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);
        var deadline = DateTime.UtcNow.Add(ProcessedTimeout);
        while (!await inbox.Find(x => x.TypeId == SlowTypeId && x.Status == "Processed").AnyAsync())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"The '{SlowTypeId}' message was not processed within {ProcessedTimeout}.");

            await Task.Delay(100);
        }
    }
}
