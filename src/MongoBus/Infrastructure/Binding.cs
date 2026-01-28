using MongoDB.Bson;

namespace MongoBus.Infrastructure;

public sealed class Binding
{
    public ObjectId Id { get; set; }
    public string Topic { get; set; } = default!; // == typeId
    public string EndpointId { get; set; } = default!;
}
