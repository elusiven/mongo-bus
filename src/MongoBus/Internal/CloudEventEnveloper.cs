using System.Diagnostics;
using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.Internal;

internal sealed class CloudEventEnveloper : ICloudEventEnveloper
{
    public CloudEventEnvelope<T> CreateEnvelope<T>(PublishContext<T> context)
    {
        var envelope = new CloudEventEnvelope<T>
        {
            Type = context.TypeId,
            Source = context.Source,
            Subject = context.Subject,
            Id = CreateId(context.Id),
            Time = ResolveTime(context.TimeUtc),
            CorrelationId = context.CorrelationId,
            CausationId = context.CausationId,
            DataContentType = context.DataContentType,
            Data = context.Data
        };

        ApplyTraceContext(envelope, Activity.Current);
        return envelope;
    }

    private static string CreateId(string? id) =>
        string.IsNullOrWhiteSpace(id) ? Guid.NewGuid().ToString("N") : id!;

    private static DateTime ResolveTime(DateTime? timeUtc) => timeUtc ?? DateTime.UtcNow;

    private static void ApplyTraceContext<T>(CloudEventEnvelope<T> envelope, Activity? activity)
    {
        if (activity == null) return;

        envelope.TraceParent = activity.Id;
        envelope.TraceState = activity.TraceStateString;
    }
}

