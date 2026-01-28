using MongoBus.Infrastructure;
using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IMessageDispatcher
{
    Task DispatchAsync(InboxMessage message, ConsumeContext context, CancellationToken ct);
}
