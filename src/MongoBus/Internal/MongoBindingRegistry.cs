using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

public sealed class MongoBindingRegistry : ITopologyManager
{
    private readonly IMongoCollection<Binding> _bindings;

    public MongoBindingRegistry(IMongoDatabase db)
    {
        _bindings = db.GetCollection<Binding>(MongoBusConstants.BindingsCollectionName);
    }

    public Task BindAsync(string endpointId, string typeId, CancellationToken ct = default)
    {
        return _bindings.UpdateOneAsync(
            BuildFilter(endpointId, typeId),
            BuildUpdate(endpointId, typeId),
            new UpdateOptions { IsUpsert = true },
            ct);
    }

    private static FilterDefinition<Binding> BuildFilter(string endpointId, string typeId) =>
        Builders<Binding>.Filter.Where(x => x.EndpointId == endpointId && x.Topic == typeId);

    private static UpdateDefinition<Binding> BuildUpdate(string endpointId, string typeId) =>
        Builders<Binding>.Update
            .SetOnInsert(x => x.EndpointId, endpointId)
            .SetOnInsert(x => x.Topic, typeId);
}
