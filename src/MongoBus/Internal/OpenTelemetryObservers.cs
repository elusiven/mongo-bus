using System.Diagnostics.Metrics;
using MongoBus.Abstractions;

namespace MongoBus.Internal;

internal sealed class OpenTelemetryPublishObserver : IPublishObserver
{
    private static readonly Counter<long> PublishCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.publish.count");
    private static readonly Counter<long> PublishFailureCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.publish.failure.count");
    private static readonly Histogram<double> PublishLatency = MongoBusDiagnostics.Meter.CreateHistogram<double>("mongobus.publish.latency.ms");

    public void OnPublish(PublishMetrics metrics)
    {
        PublishCount.Add(1, new KeyValuePair<string, object?>("type", metrics.TypeId));
        PublishLatency.Record(metrics.Latency.TotalMilliseconds, new KeyValuePair<string, object?>("type", metrics.TypeId));
    }

    public void OnPublishFailed(PublishFailureMetrics metrics)
    {
        PublishFailureCount.Add(1, new KeyValuePair<string, object?>("type", metrics.TypeId));
        PublishLatency.Record(metrics.Latency.TotalMilliseconds, new KeyValuePair<string, object?>("type", metrics.TypeId));
    }
}

internal sealed class OpenTelemetryConsumeObserver : IConsumeObserver
{
    private static readonly Counter<long> ConsumeCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.consume.count");
    private static readonly Counter<long> ConsumeFailureCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.consume.failure.count");
    private static readonly Histogram<double> ConsumeLatency = MongoBusDiagnostics.Meter.CreateHistogram<double>("mongobus.consume.latency.ms");

    public void OnMessageProcessed(ConsumeMetrics metrics)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("endpoint", metrics.Context.EndpointId),
            new("type", metrics.Context.TypeId)
        };
        ConsumeCount.Add(1, tags);
        ConsumeLatency.Record(metrics.Latency.TotalMilliseconds, tags);
    }

    public void OnMessageFailed(ConsumeFailureMetrics metrics)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("endpoint", metrics.Context.EndpointId),
            new("type", metrics.Context.TypeId)
        };
        ConsumeFailureCount.Add(1, tags);
        ConsumeLatency.Record(metrics.Latency.TotalMilliseconds, tags);
    }
}

internal sealed class OpenTelemetryBatchObserver : IBatchObserver
{
    private static readonly Counter<long> BatchCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.batch.count");
    private static readonly Counter<long> BatchFailureCount = MongoBusDiagnostics.Meter.CreateCounter<long>("mongobus.batch.failure.count");
    private static readonly Histogram<double> BatchLatency = MongoBusDiagnostics.Meter.CreateHistogram<double>("mongobus.batch.latency.ms");
    private static readonly Histogram<int> BatchSize = MongoBusDiagnostics.Meter.CreateHistogram<int>("mongobus.batch.size");

    public void OnBatchProcessed(BatchMetrics metrics)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("endpoint", metrics.EndpointId),
            new("type", metrics.TypeId),
            new("group", metrics.GroupKey),
            new("flush", metrics.FlushMode.ToString())
        };
        BatchCount.Add(1, tags);
        BatchSize.Record(metrics.BatchSize, tags);
        BatchLatency.Record(metrics.Latency.TotalMilliseconds, tags);
    }

    public void OnBatchFailed(BatchFailureMetrics metrics)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("endpoint", metrics.EndpointId),
            new("type", metrics.TypeId),
            new("failure", metrics.FailureMode.ToString())
        };
        BatchFailureCount.Add(1, tags);
        BatchLatency.Record(metrics.Latency.TotalMilliseconds, tags);
    }
}
