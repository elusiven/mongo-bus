namespace MongoBus.Abstractions;

public interface IMessageBus
{
    Task PublishAsync<T>(
        string typeId,
        T data,
        string? source = null,
        string? subject = null,
        string? id = null,
        DateTime? timeUtc = null,
        DateTime? deliverAt = null,
        string? correlationId = null,
        string? causationId = null,
        CancellationToken ct = default);
}
