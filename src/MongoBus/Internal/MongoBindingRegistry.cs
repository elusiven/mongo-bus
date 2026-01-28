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
        var filter = Builders<Binding>.Filter.Where(x => x.EndpointId == endpointId && x.Topic == typeId);
        var update = Builders<Binding>.Update
            .SetOnInsert(x => x.EndpointId, endpointId)
            .SetOnInsert(x => x.Topic, typeId);

        return _bindings.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, ct);
    }
}
