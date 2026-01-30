namespace MongoBus.Abstractions;

public interface IConsumeObserver
{
    void OnMessageProcessed(ConsumeMetrics metrics);
    void OnMessageFailed(ConsumeFailureMetrics metrics);
}
