using Docker.DotNet.Models;
using Testcontainers.MongoDb;
using Xunit;

namespace MongoBus.Tests;

/// <summary>
/// A single-node MongoDB replica set. Transactions require a replica set, and the shared
/// <see cref="MongoDbFixture"/> is a standalone server so it can also cover deployments without transactions.
/// </summary>
public class MongoReplicaSetFixture : IAsyncLifetime
{
    private const long OpenFileLimit = 64000;

    // Test commands allow the failCommand failpoint, so tests can make MongoDB fail operations in a transaction.
    public MongoDbContainer Container { get; } = new MongoDbBuilder("mongo:6.0")
        .WithReplicaSet("rs0")
        .WithCreateParameterModifier(parameters =>
            parameters.HostConfig.Ulimits = [new Ulimit { Name = "nofile", Soft = OpenFileLimit, Hard = OpenFileLimit }])
        .WithCommand("--setParameter", "enableTestCommands=1")
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

[CollectionDefinition("Mongo replica set collection")]
public class MongoReplicaSetCollection : ICollectionFixture<MongoReplicaSetFixture>
{
}
