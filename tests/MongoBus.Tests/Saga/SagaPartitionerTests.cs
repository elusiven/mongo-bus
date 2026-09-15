using FluentAssertions;
using MongoBus.Internal.Saga;
using Xunit;

namespace MongoBus.Tests.Saga;

public class SagaPartitionerTests
{
    [Fact]
    public async Task AcquireAsync_SameKey_Serializes()
    {
        var partitioner = new SagaPartitioner(4);
        var order = new List<int>();

        var lock1 = await partitioner.AcquireAsync("key-a", CancellationToken.None);

        var task = Task.Run(async () =>
        {
            using var lock2 = await partitioner.AcquireAsync("key-a", CancellationToken.None);
            order.Add(2);
        });

        // Give the task time to block on the same key
        await Task.Delay(200);
        order.Add(1);
        lock1.Dispose();

        await task;

        order.Should().Equal(new[] { 1, 2 }, "second acquire should block until first lock is released");
    }

    [Theory]
    [InlineData(int.MinValue, 4)]
    [InlineData(int.MaxValue, 4)]
    [InlineData(-1, 4)]
    [InlineData(0, 4)]
    [InlineData(int.MinValue, 1)]
    public void GetPartitionIndex_ReturnsValidIndex_ForAnyHashCode(int hashCode, int partitionCount)
    {
        var index = SagaPartitioner.GetPartitionIndex(hashCode, partitionCount);

        index.Should().BeInRange(0, partitionCount - 1);
    }

    [Fact]
    public async Task AcquireAsync_KeysInDifferentPartitions_CanRunConcurrently()
    {
        const int partitionCount = 4;
        var partitioner = new SagaPartitioner(partitionCount);
        var (firstKey, secondKey) = KeysInDifferentPartitions(partitionCount);
        var acquired = new List<string>();
        var gate = new ManualResetEventSlim(false);

        var task1 = Task.Run(async () =>
        {
            using var lock1 = await partitioner.AcquireAsync(firstKey, CancellationToken.None);
            lock (acquired) { acquired.Add(firstKey); }
            gate.Wait(TimeSpan.FromSeconds(5));
        });

        var task2 = Task.Run(async () =>
        {
            using var lock2 = await partitioner.AcquireAsync(secondKey, CancellationToken.None);
            lock (acquired) { acquired.Add(secondKey); }
            gate.Wait(TimeSpan.FromSeconds(5));
        });

        // Wait briefly for both tasks to acquire their locks
        await Task.Delay(300);

        lock (acquired)
        {
            acquired.Should().HaveCount(2, "keys in different partitions should be acquired concurrently without blocking");
            acquired.Should().Contain(firstKey);
            acquired.Should().Contain(secondKey);
        }

        gate.Set();
        await Task.WhenAll(task1, task2);
    }

    /// <summary>
    /// String hash codes are randomised per process, so two fixed keys share a partition in some test runs.
    /// Choosing the second key by its partition in this process keeps the test deterministic.
    /// </summary>
    private static (string First, string Second) KeysInDifferentPartitions(int partitionCount)
    {
        const string firstKey = "key-0";
        var firstPartition = SagaPartitioner.GetPartitionIndex(firstKey.GetHashCode(), partitionCount);
        var secondKey = Enumerable.Range(1, 1000)
            .Select(i => $"key-{i}")
            .First(key => SagaPartitioner.GetPartitionIndex(key.GetHashCode(), partitionCount) != firstPartition);

        return (firstKey, secondKey);
    }
}
