using Testcontainers.MongoDb;
using Xunit;

namespace MongoBus.Tests;

public class MongoDbFixture : IAsyncLifetime
{
    public MongoDbContainer Container { get; } = new MongoDbBuilder("mongo:6.0")
        .Build();

    public string ConnectionString => Container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await Container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await Container.StopAsync();
    }
}

[CollectionDefinition("Mongo collection")]
public class MongoCollection : ICollectionFixture<MongoDbFixture>
{
}
