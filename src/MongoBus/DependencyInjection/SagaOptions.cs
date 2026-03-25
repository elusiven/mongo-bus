using MongoBus.Models;

namespace MongoBus.DependencyInjection;

public enum ExceptionRetryMode
{
    DenyList,
    AllowList
}

public sealed class SagaOptions
{
    public int ConcurrencyLimit { get; set; } = 16;
    public int PrefetchCount { get; set; } = 64;
    public TimeSpan LockTime { get; set; } = TimeSpan.FromSeconds(60);
    public int MaxAttempts { get; set; } = 10;
    public bool IdempotencyEnabled { get; set; }
    public string? EndpointName { get; set; }

    public int DefaultPartitionCount { get; set; }
    public Func<ConsumeContext, string>? DefaultPartitionKeySelector { get; set; }

    public TimeSpan SagaInstanceTtl { get; set; } = TimeSpan.Zero;

    public bool HistoryEnabled { get; set; }
    public TimeSpan HistoryTtl { get; set; } = TimeSpan.FromDays(30);

    public ExceptionRetryMode RetryMode { get; set; } = ExceptionRetryMode.DenyList;
    public List<Type> NoRetryExceptions { get; set; } = [];
    public List<Type> RetryExceptions { get; set; } = [];

    public bool ShouldRetry(Exception ex)
    {
        var exType = ex.GetType();

        return RetryMode switch
        {
            ExceptionRetryMode.DenyList =>
                !NoRetryExceptions.Any(t => t.IsAssignableFrom(exType)),
            ExceptionRetryMode.AllowList =>
                RetryExceptions.Any(t => t.IsAssignableFrom(exType)),
            _ => true
        };
    }
}
