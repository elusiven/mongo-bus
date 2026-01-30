namespace MongoBus.Abstractions;

public interface IPublishObserver
{
    void OnPublish(PublishMetrics metrics);
    void OnPublishFailed(PublishFailureMetrics metrics);
}
