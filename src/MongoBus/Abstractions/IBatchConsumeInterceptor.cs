using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IBatchConsumeInterceptor
{
    Task OnConsumeBatchAsync(BatchConsumeContext context, IReadOnlyList<object> messages, Func<Task> next, CancellationToken ct);
}
