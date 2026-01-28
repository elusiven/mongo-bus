using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface IMessageHandler<in T>
{
    Task HandleAsync(T message, ConsumeContext context, CancellationToken ct);
}
