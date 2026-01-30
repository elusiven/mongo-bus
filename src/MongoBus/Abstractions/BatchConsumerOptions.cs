namespace MongoBus.Abstractions;

public enum BatchFailureMode
{
    RetryBatch,
    MarkDead
}

public enum BatchFlushMode
{
    SinceFirstMessage,
    SinceLastMessage
}

public sealed record BatchConsumerOptions
{
    public int MinBatchSize { get; init; } = 1;
    public int MaxBatchSize { get; init; } = 50;
    public TimeSpan MaxBatchWaitTime { get; init; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan MaxBatchIdleTime { get; init; } = TimeSpan.Zero;
    public BatchFlushMode FlushMode { get; init; } = BatchFlushMode.SinceFirstMessage;
    public BatchFailureMode FailureMode { get; init; } = BatchFailureMode.RetryBatch;

    public void EnsureValid()
    {
        if (MinBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MinBatchSize), "MinBatchSize must be >= 1.");
        if (MaxBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxBatchSize), "MaxBatchSize must be >= 1.");
        if (MaxBatchSize < MinBatchSize)
            throw new ArgumentOutOfRangeException(nameof(MaxBatchSize), "MaxBatchSize must be >= MinBatchSize.");
        if (FlushMode == BatchFlushMode.SinceFirstMessage)
        {
            if (MaxBatchWaitTime <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(MaxBatchWaitTime), "MaxBatchWaitTime must be > 0 when FlushMode is SinceFirstMessage.");
            if (MaxBatchIdleTime != TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(MaxBatchIdleTime), "MaxBatchIdleTime must be 0 when FlushMode is SinceFirstMessage.");
        }
        else
        {
            if (MaxBatchIdleTime <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(MaxBatchIdleTime), "MaxBatchIdleTime must be > 0 when FlushMode is SinceLastMessage.");
            if (MaxBatchWaitTime != TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(MaxBatchWaitTime), "MaxBatchWaitTime must be 0 when FlushMode is SinceLastMessage.");
        }
    }
}
