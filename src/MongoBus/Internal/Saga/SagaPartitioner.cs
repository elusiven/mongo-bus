namespace MongoBus.Internal.Saga;

internal sealed class SagaPartitioner
{
    private readonly SemaphoreSlim[] _partitions;

    public SagaPartitioner(int partitionCount)
    {
        if (partitionCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be greater than 0.");

        _partitions = new SemaphoreSlim[partitionCount];
        for (var i = 0; i < partitionCount; i++)
            _partitions[i] = new SemaphoreSlim(1, 1);
    }

    public int PartitionCount => _partitions.Length;

    public async Task<IDisposable> AcquireAsync(string key, CancellationToken ct)
    {
        var index = Math.Abs(key.GetHashCode()) % _partitions.Length;
        await _partitions[index].WaitAsync(ct);
        return new PartitionLock(_partitions[index]);
    }

    private sealed class PartitionLock(SemaphoreSlim semaphore) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (!_disposed)
            {
                semaphore.Release();
                _disposed = true;
            }
        }
    }
}
