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
    public async Task AcquireAsync_DifferentKeys_CanRunConcurrently()
    {
        var partitioner = new SagaPartitioner(4);
        var acquired = new List<string>();
        var gate = new ManualResetEventSlim(false);

        var task1 = Task.Run(async () =>
        {
            using var lock1 = await partitioner.AcquireAsync("key-x", CancellationToken.None);
            lock (acquired) { acquired.Add("key-x"); }
            gate.Wait(TimeSpan.FromSeconds(5));
        });

        var task2 = Task.Run(async () =>
        {
            using var lock2 = await partitioner.AcquireAsync("key-y", CancellationToken.None);
            lock (acquired) { acquired.Add("key-y"); }
            gate.Wait(TimeSpan.FromSeconds(5));
        });

        // Wait briefly for both tasks to acquire their locks
        await Task.Delay(300);

        lock (acquired)
        {
            acquired.Should().HaveCount(2, "both keys should be acquired concurrently without blocking");
            acquired.Should().Contain("key-x");
            acquired.Should().Contain("key-y");
        }

        gate.Set();
        await Task.WhenAll(task1, task2);
    }
}
