using MongoBus.Models;

namespace MongoBus.Abstractions;

public interface ICloudEventEnveloper
{
    CloudEventEnvelope<T> CreateEnvelope<T>(PublishContext<T> context);
}
