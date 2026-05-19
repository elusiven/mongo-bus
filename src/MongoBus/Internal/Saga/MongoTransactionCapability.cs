using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

/// <summary>
/// One-shot lazy probe for whether the connected MongoDB deployment supports
/// multi-document transactions. Transactions require a replica set or sharded
/// cluster — the <c>hello</c> response carries <c>setName</c> on replica-set
/// members. Cached after first call so we don't probe per event.
/// </summary>
internal sealed class MongoTransactionCapability(IMongoClient client)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool? _cached;

    public async Task<bool> IsSupportedAsync(CancellationToken ct)
    {
        if (_cached is { } cached)
            return cached;

        await _gate.WaitAsync(ct);
        try
        {
            if (_cached is { } cached2)
                return cached2;

            var admin = client.GetDatabase("admin");
            var hello = await admin.RunCommandAsync<BsonDocument>(
                new BsonDocument("hello", 1), cancellationToken: ct);

            // setName is present on replica-set members; the hello response from a mongos
            // router carries msg=isdbgrid. Either supports transactions.
            var supported = hello.Contains("setName")
                || (hello.TryGetValue("msg", out var msg) && msg.IsString && msg.AsString == "isdbgrid");
            _cached = supported;
            return supported;
        }
        finally
        {
            _gate.Release();
        }
    }
}
