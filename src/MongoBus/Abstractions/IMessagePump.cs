using MongoBus.Infrastructure;

namespace MongoBus.Abstractions;

public interface IMessagePump
{
    Task<InboxMessage?> TryLockOneAsync(string endpointId, TimeSpan lockTime, string pumpId, CancellationToken ct);
    Task<InboxMessage?> TryLockOneAsync(string endpointId, IReadOnlyCollection<string> typeIds, TimeSpan lockTime, string pumpId, CancellationToken ct);
}
