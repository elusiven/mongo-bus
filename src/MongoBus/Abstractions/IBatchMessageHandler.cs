using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IBatchMessageHandler<in T>
{
    Task HandleBatchAsync(IReadOnlyList<T> messages, BatchConsumeContext context, CancellationToken ct);
}
