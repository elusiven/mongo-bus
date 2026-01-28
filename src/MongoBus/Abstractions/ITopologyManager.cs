namespace MongoBus.Abstractions;

public interface ITopologyManager
{
    Task BindAsync(string endpointId, string typeId, CancellationToken ct = default);
}
