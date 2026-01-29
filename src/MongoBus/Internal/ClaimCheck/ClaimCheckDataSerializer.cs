using System.Text.Json;

namespace MongoBus.Internal.ClaimCheck;

internal interface IClaimCheckDataSerializer
{
    Task SerializeAsync(object data, Stream target, CancellationToken ct);
    Task<object> DeserializeAsync(Stream source, Type type, CancellationToken ct);
}

internal sealed class ClaimCheckDataSerializer : IClaimCheckDataSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public Task SerializeAsync(object data, Stream target, CancellationToken ct)
        => JsonSerializer.SerializeAsync(target, data, data.GetType(), Options, ct);

    public async Task<object> DeserializeAsync(Stream source, Type type, CancellationToken ct)
    {
        var result = await JsonSerializer.DeserializeAsync(source, type, Options, ct);
        return result!;
    }
}
