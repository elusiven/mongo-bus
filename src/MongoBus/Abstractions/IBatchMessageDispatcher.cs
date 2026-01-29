using MongoBus.Infrastructure;
using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IBatchMessageDispatcher
{
    Task DispatchBatchAsync(IReadOnlyList<InboxMessage> messages, BatchConsumeContext context, CancellationToken ct);
}
