using System.Text.Json;
using MongoBus.Abstractions;

namespace MongoBus.Internal;

internal sealed class CloudEventSerializer : ICloudEventSerializer
{
    private static readonly JsonSerializerOptions Options = CreateOptions();

    public string Serialize<T>(T envelope) => JsonSerializer.Serialize(envelope, Options);

    public T Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options)!;

    public object Deserialize(string json, Type type) => JsonSerializer.Deserialize(json, type, Options)!;

    public JsonDocument Parse(string json) => JsonDocument.Parse(json);

    private static JsonSerializerOptions CreateOptions() => new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}
