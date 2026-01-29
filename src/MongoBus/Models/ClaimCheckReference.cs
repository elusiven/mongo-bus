using System.Text.Json.Serialization;

namespace MongoBus.Models;

public sealed record ClaimCheckReference(
    [property: JsonPropertyName("provider")] string Provider,
    [property: JsonPropertyName("container")] string Container,
    [property: JsonPropertyName("key")] string Key,
    [property: JsonPropertyName("length")] long Length,
    [property: JsonPropertyName("contentType")] string? ContentType = null,
    [property: JsonPropertyName("metadata")] IReadOnlyDictionary<string, string>? Metadata = null,
    [property: JsonPropertyName("createdAt")] DateTime? CreatedAt = null);
