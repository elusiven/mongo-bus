namespace MongoBus.Abstractions;

public interface IBatchObserver
{
    void OnBatchProcessed(BatchMetrics metrics);
    void OnBatchFailed(BatchFailureMetrics metrics);
}
