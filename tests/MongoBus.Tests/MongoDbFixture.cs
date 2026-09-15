using Docker.DotNet.Models;
using Testcontainers.MongoDb;
using Xunit;

namespace MongoBus.Tests;

public class MongoDbFixture : IAsyncLifetime
{
    // Tests share this server but each uses its own database, and every collection and index is a
    // separate WiredTiger file. Docker's default soft limit of 1024 open files runs out part-way
    // through the suite; mongod then aborts and every later test times out on server selection.
    private const long OpenFileLimit = 64000;

    public MongoDbContainer Container { get; } = new MongoDbBuilder("mongo:6.0")
        .WithCreateParameterModifier(parameters =>
            parameters.HostConfig.Ulimits = [new Ulimit { Name = "nofile", Soft = OpenFileLimit, Hard = OpenFileLimit }])
        .Build();

    public string ConnectionString => Container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await Container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await Container.StopAsync();
    }
}

[CollectionDefinition("Mongo collection")]
public class MongoCollection : ICollectionFixture<MongoDbFixture>
{
}
