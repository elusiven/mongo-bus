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

    public TimeSpan SagaTimeout { get; set; } = TimeSpan.Zero;
    public string TimeoutStateName { get; set; } = "TimedOut";
    public TimeSpan TimeoutScanInterval { get; set; } = TimeSpan.FromSeconds(30);

    public ExceptionRetryMode RetryMode { get; set; } = ExceptionRetryMode.DenyList;
    public List<Type> NoRetryExceptions { get; set; } = [];
    public List<Type> RetryExceptions { get; set; } = [];

    /// <summary>
    /// When true, saga publishes from <c>.Publish</c> / <c>.Send</c> activities are buffered
    /// during the activity chain and written atomically with the saga state inside a single
    /// MongoDB transaction. If the saga write aborts, no outbox rows are committed and no
    /// downstream publishes escape.
    ///
    /// Requires a MongoDB replica set or sharded cluster (transactions are not supported on
    /// standalone instances) and <c>MongoBusOptions.Outbox.Enabled = true</c>.
    /// Defaults to <c>false</c> — today's direct-publish behavior is preserved.
    /// </summary>
    public bool UseOutbox { get; set; }

    /// <summary>
    /// Only consulted when <see cref="UseOutbox"/> is true. If the connected MongoDB
    /// deployment does not support transactions and this flag is true, the runtime logs a
    /// warning and falls back to direct publishing (the pre-<c>UseOutbox</c> semantic).
    /// Defaults to <c>false</c> — running against a standalone Mongo with
    /// <c>UseOutbox = true</c> surfaces the underlying driver error instead, so deployments
    /// can't silently degrade their durability guarantees in production.
    /// </summary>
    public bool AllowFallbackWhenTransactionsUnsupported { get; set; }

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
