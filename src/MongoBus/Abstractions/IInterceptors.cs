using MongoBus.Models;

namespace MongoBus.Abstractions;

/// <summary>
/// Interceptor for the message publishing pipeline.
/// </summary>
public interface IPublishInterceptor
{
    Task OnPublishAsync<T>(PublishContext<T> context, Func<Task> next, CancellationToken ct);
}

/// <summary>
/// Interceptor for the message consuming pipeline.
/// </summary>
public interface IConsumeInterceptor
{
    Task OnConsumeAsync(ConsumeContext context, object message, Func<Task> next, CancellationToken ct);
}

public record PublishContext<T>(
    string TypeId,
    T Data,
    string Source,
    string? Subject = null,
    string? Id = null,
    DateTime? TimeUtc = null,
    DateTime? DeliverAt = null,
    string? CorrelationId = null,
    string? CausationId = null)
{
    // Allow interceptors to modify some of these if needed, 
    // though for simplicity we might keep them immutable in the record but allows returning a new one if we wanted to.
    // For now, let's keep it simple.
}
