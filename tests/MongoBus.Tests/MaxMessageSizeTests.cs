using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class MaxMessageSizeTests(MongoDbFixture fixture)
{
    public sealed record BigMessage(string Value);

    [Fact]
    public async Task PublishAsync_ShouldThrow_WhenPayloadExceedsMaxMessageSize()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "maxsize_test_" + Guid.NewGuid().ToString("N");
            opt.MaxMessageSizeBytes = 512; // tiny cap, claim-check disabled
        });

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        // Seed a binding so the publish has a route and actually builds a payload.
        await db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName)
            .InsertOneAsync(new Binding { Topic = "big.message", EndpointId = "e1" });

        var oversized = new BigMessage(new string('x', 5000));

        var act = () => bus.PublishAsync("big.message", oversized);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*size*");
    }

    [Fact]
    public async Task PublishAsync_ShouldSucceed_WhenPayloadWithinMaxMessageSize()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "maxsize_ok_test_" + Guid.NewGuid().ToString("N");
            opt.MaxMessageSizeBytes = 16 * 1024 * 1024;
        });

        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IMongoDatabase>();
        var bus = sp.GetRequiredService<IMessageBus>();

        await db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName)
            .InsertOneAsync(new Binding { Topic = "small.message", EndpointId = "e1" });

        var act = () => bus.PublishAsync("small.message", new BigMessage("hello"));

        await act.Should().NotThrowAsync();
    }
}
