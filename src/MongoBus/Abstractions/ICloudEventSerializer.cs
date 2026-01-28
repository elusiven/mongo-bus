using System.Text.Json;

namespace MongoBus.Abstractions;

public interface ICloudEventSerializer
{
    string Serialize<T>(T envelope);
    T Deserialize<T>(string json);
    object Deserialize(string json, Type type);
    JsonDocument Parse(string json);
}
