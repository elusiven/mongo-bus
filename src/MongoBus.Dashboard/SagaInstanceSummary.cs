using MongoDB.Bson;

namespace MongoBus.Dashboard;

/// <summary>
/// The JSON shape of a saga instance in the dashboard API. Raw <see cref="BsonDocument"/>s cannot be
/// serialized by System.Text.Json, so the endpoint maps each instance to this record.
/// </summary>
internal sealed record SagaInstanceSummary(
    string? CorrelationId,
    string? CurrentState,
    int? Version,
    DateTime? CreatedUtc,
    DateTime? LastModifiedUtc)
{
    public static SagaInstanceSummary From(BsonDocument instance) => new(
        StringField(instance, "CorrelationId"),
        StringField(instance, "CurrentState"),
        IntField(instance, "Version"),
        DateTimeField(instance, "CreatedUtc"),
        DateTimeField(instance, "LastModifiedUtc"));

    private static string? StringField(BsonDocument instance, string name) =>
        instance.TryGetValue(name, out var value) && value.IsString ? value.AsString : null;

    private static int? IntField(BsonDocument instance, string name) =>
        instance.TryGetValue(name, out var value) && value.IsNumeric ? value.ToInt32() : null;

    private static DateTime? DateTimeField(BsonDocument instance, string name) =>
        instance.TryGetValue(name, out var value) && value.IsValidDateTime ? value.ToUniversalTime() : null;
}
