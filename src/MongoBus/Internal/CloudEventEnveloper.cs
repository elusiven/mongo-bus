using System.Diagnostics;
using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.Internal;

internal sealed class CloudEventEnveloper : ICloudEventEnveloper
{
    public CloudEventEnvelope<T> CreateEnvelope<T>(PublishContext<T> context)
    {
        var activity = Activity.Current;
        
        var envelope = new CloudEventEnvelope<T>
        {
            Type = context.TypeId,
            Source = context.Source,
            Subject = context.Subject,
            Id = string.IsNullOrWhiteSpace(context.Id) ? Guid.NewGuid().ToString("N") : context.Id!,
            Time = context.TimeUtc ?? DateTime.UtcNow,
            CorrelationId = context.CorrelationId,
            CausationId = context.CausationId,
            Data = context.Data
        };

        if (activity != null)
        {
            envelope.TraceParent = activity.Id;
            envelope.TraceState = activity.TraceStateString;
        }

        return envelope;
    }
}
