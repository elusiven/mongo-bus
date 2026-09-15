using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Internal.ClaimCheck;

/// <summary>
/// Reads the keys of the claim-check payloads that inbox and outbox messages reference.
/// </summary>
internal sealed class ClaimCheckReferenceReader(IMongoDatabase db, ICloudEventSerializer serializer, ILogger log)
{
    private const string PayloadField = nameof(InboxMessage.PayloadJson);

    private static readonly string[] MessageCollectionNames =
        [MongoBusConstants.InboxCollectionName, MongoBusConstants.OutboxCollectionName];

    // Payloads are stored as JSON text, so messages that may reference a claim check can only be found by searching
    // for its content type. The "+json" suffix is left out because System.Text.Json writes '+' as +.
    private static readonly FilterDefinition<BsonDocument> MayReferenceClaimCheck =
        Builders<BsonDocument>.Filter.Regex(PayloadField, new BsonRegularExpression(Regex.Escape("vnd.mongobus.claim-check"), "i"));

    private static readonly ProjectionDefinition<BsonDocument> IdAndPayload =
        Builders<BsonDocument>.Projection.Include(PayloadField);

    public async IAsyncEnumerable<string> ReadReferencedKeysAsync([EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var collectionName in MessageCollectionNames)
        {
            await foreach (var message in FindMessagesThatMayReferenceClaimChecksAsync(collectionName, ct))
            {
                if (TryReadReferencedKey(message, collectionName, out var key))
                    yield return key;
            }
        }
    }

    private async IAsyncEnumerable<BsonDocument> FindMessagesThatMayReferenceClaimChecksAsync(
        string collectionName,
        [EnumeratorCancellation] CancellationToken ct)
    {
        using var cursor = await db.GetCollection<BsonDocument>(collectionName)
            .Find(MayReferenceClaimCheck)
            .Project(IdAndPayload)
            .ToCursorAsync(ct);

        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var message in cursor.Current)
                yield return message;
        }
    }

    private bool TryReadReferencedKey(BsonDocument message, string collectionName, [NotNullWhen(true)] out string? key)
    {
        try
        {
            return TryReadReferencedKey(message[PayloadField].AsString, out key);
        }
        catch (JsonException ex)
        {
            // A reference the bus cannot read cannot be resolved by consumers either, so the payload it names is unreachable.
            log.LogWarning(ex, "Ignoring message {MessageId} in {Collection} during claim-check cleanup because its payload cannot be read",
                message["_id"], collectionName);
            key = null;
            return false;
        }
    }

    private bool TryReadReferencedKey(string payloadJson, [NotNullWhen(true)] out string? key)
    {
        using var envelope = serializer.Parse(payloadJson);
        key = IsClaimCheckEnvelope(envelope.RootElement)
            ? serializer.Deserialize<ClaimCheckReference>(envelope.RootElement.GetProperty("data").GetRawText()).Key
            : null;
        return key is not null;
    }

    private static bool IsClaimCheckEnvelope(JsonElement root) =>
        root.ValueKind == JsonValueKind.Object
        && root.TryGetProperty("dataContentType", out var contentType)
        && contentType.ValueKind == JsonValueKind.String
        && string.Equals(contentType.GetString(), ClaimCheckConstants.ContentType, StringComparison.OrdinalIgnoreCase)
        && root.TryGetProperty("data", out var data)
        && data.ValueKind == JsonValueKind.Object;
}
