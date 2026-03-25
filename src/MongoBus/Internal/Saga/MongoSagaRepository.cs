using System.Linq.Expressions;
using MongoBus.Abstractions.Saga;
using MongoBus.Models.Saga;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

internal interface ISagaRepository<TInstance>
    where TInstance : class, ISagaInstance
{
    Task<TInstance?> FindAsync(string correlationId, CancellationToken ct);
    Task<TInstance?> FindByPropertyAsync<TProperty>(Expression<Func<TInstance, TProperty>> property, TProperty value, CancellationToken ct);
    Task InsertAsync(TInstance instance, CancellationToken ct);
    Task UpdateAsync(TInstance instance, int expectedVersion, CancellationToken ct);
    Task DeleteAsync(string correlationId, CancellationToken ct);
}

internal sealed class MongoSagaRepository<TInstance>(IMongoCollection<TInstance> collection)
    : ISagaRepository<TInstance>
    where TInstance : class, ISagaInstance
{
    public async Task<TInstance?> FindAsync(string correlationId, CancellationToken ct)
    {
        return await collection.Find(x => x.CorrelationId == correlationId)
            .FirstOrDefaultAsync(ct);
    }

    public async Task<TInstance?> FindByPropertyAsync<TProperty>(
        Expression<Func<TInstance, TProperty>> property,
        TProperty value,
        CancellationToken ct)
    {
        var filter = Builders<TInstance>.Filter.Eq(property, value);
        return await collection.Find(filter).FirstOrDefaultAsync(ct);
    }

    public async Task InsertAsync(TInstance instance, CancellationToken ct)
    {
        await collection.InsertOneAsync(instance, cancellationToken: ct);
    }

    public async Task UpdateAsync(TInstance instance, int expectedVersion, CancellationToken ct)
    {
        var filter = Builders<TInstance>.Filter.And(
            Builders<TInstance>.Filter.Eq(x => x.CorrelationId, instance.CorrelationId),
            Builders<TInstance>.Filter.Eq(x => x.Version, expectedVersion));

        var result = await collection.ReplaceOneAsync(filter, instance, cancellationToken: ct);

        if (result.MatchedCount == 0)
        {
            throw new SagaConcurrencyException(instance.CorrelationId, expectedVersion);
        }
    }

    public async Task DeleteAsync(string correlationId, CancellationToken ct)
    {
        await collection.DeleteOneAsync(x => x.CorrelationId == correlationId, ct);
    }
}
