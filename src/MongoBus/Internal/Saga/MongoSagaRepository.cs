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
    Task<TInstance?> FindByPropertyNameAsync(string propertyName, string value, CancellationToken ct);
    Task InsertAsync(TInstance instance, CancellationToken ct);
    Task InsertAsync(TInstance instance, IClientSessionHandle session, CancellationToken ct);
    Task UpdateAsync(TInstance instance, int expectedVersion, CancellationToken ct);
    Task UpdateAsync(TInstance instance, int expectedVersion, IClientSessionHandle session, CancellationToken ct);
    Task DeleteAsync(string correlationId, CancellationToken ct);
    Task DeleteAsync(string correlationId, IClientSessionHandle session, CancellationToken ct);
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

    public async Task<TInstance?> FindByPropertyNameAsync(
        string propertyName,
        string value,
        CancellationToken ct)
    {
        var filter = Builders<TInstance>.Filter.Eq(propertyName, value);
        return await collection.Find(filter).FirstOrDefaultAsync(ct);
    }

    public Task InsertAsync(TInstance instance, CancellationToken ct)
        => collection.InsertOneAsync(instance, cancellationToken: ct);

    public Task InsertAsync(TInstance instance, IClientSessionHandle session, CancellationToken ct)
        => collection.InsertOneAsync(session, instance, cancellationToken: ct);

    public Task UpdateAsync(TInstance instance, int expectedVersion, CancellationToken ct)
        => ReplaceWithVersionCheckAsync(instance, expectedVersion, session: null, ct);

    public Task UpdateAsync(TInstance instance, int expectedVersion, IClientSessionHandle session, CancellationToken ct)
        => ReplaceWithVersionCheckAsync(instance, expectedVersion, session, ct);

    public Task DeleteAsync(string correlationId, CancellationToken ct)
        => collection.DeleteOneAsync(x => x.CorrelationId == correlationId, ct);

    public Task DeleteAsync(string correlationId, IClientSessionHandle session, CancellationToken ct)
        => collection.DeleteOneAsync(session, Builders<TInstance>.Filter.Eq(x => x.CorrelationId, correlationId), cancellationToken: ct);

    private async Task ReplaceWithVersionCheckAsync(
        TInstance instance,
        int expectedVersion,
        IClientSessionHandle? session,
        CancellationToken ct)
    {
        var filter = Builders<TInstance>.Filter.And(
            Builders<TInstance>.Filter.Eq(x => x.CorrelationId, instance.CorrelationId),
            Builders<TInstance>.Filter.Eq(x => x.Version, expectedVersion));

        var result = session is null
            ? await collection.ReplaceOneAsync(filter, instance, cancellationToken: ct)
            : await collection.ReplaceOneAsync(session, filter, instance, cancellationToken: ct);

        if (result.MatchedCount == 0)
        {
            throw new SagaConcurrencyException(instance.CorrelationId, expectedVersion);
        }
    }
}
