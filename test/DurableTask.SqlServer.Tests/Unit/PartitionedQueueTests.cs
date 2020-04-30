namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.SqlServer.Utils;
    using Xunit;

    public class PartitionedQueueTests
    {
        [Fact(Skip = "Not used")]
        public async Task BasicPartitioning()
        {
            var queue = new PartitionedQueue<int, int>(i => i % 10);
            for (int i = 0; i < 100; i++)
            {
                queue.Add(i);
            }

            for (int i = 0; i < 10; i++)
            {
                IReadOnlyList<int> partition = await queue.TakeNextPartitionAsync(CancellationToken.None);
                Assert.Equal(10, partition.Count);
                Assert.Equal(10 - i - 1, queue.PartitionCount);

                for (int j = 0; j < partition.Count; j++)
                {
                    Assert.Equal((j * partition.Count) + i, partition[j]);
                }
            }

            Assert.Equal(0, queue.PartitionCount);
        }

        [Theory(Skip = "Not used")]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(16)]
        public void ConcurrentAddsAndTakes_Isolated(int maxDegreesOfParallelism)
        {
            var options = new ParallelOptions
            { 
                MaxDegreeOfParallelism = maxDegreesOfParallelism
            };

            int itemCount = 1000;
            int partitionCount = 20;

            var queue = new PartitionedQueue<int, int>(i => i % partitionCount);
            Parallel.For(0, itemCount, options, i => queue.Add(i));
            Assert.Equal(partitionCount, queue.PartitionCount);

            var output = new ConcurrentDictionary<int, object>();

            Parallel.For(0, partitionCount, options, _ =>
            {
                IReadOnlyList<int> partition = queue.TakeNextPartitionAsync(CancellationToken.None).GetAwaiter().GetResult();

                int partitionId = partition[0] % partitionCount;
                for (int j = 0; j < partition.Count; j++)
                {
                    Assert.Equal(partitionId, partition[j] % partitionCount);
                    Assert.True(output.TryAdd(partition[j], null));
                }
            });

            Assert.Equal(0, queue.PartitionCount);
            Assert.Equal(itemCount, output.Count);
            Assert.Equal(Enumerable.Range(0, itemCount), output.Keys.OrderBy(key => key));
        }

        [Theory(Skip = "Not used")]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(16)]
        public async Task ConcurrentAddsAndTakes_Overlapping(int maxDegreesOfParallelism)
        {
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = maxDegreesOfParallelism
            };

            int itemCount = 1000;
            int partitionCount = 32;

            var queue = new PartitionedQueue<int, int>(i => i % partitionCount);
            var output = new ConcurrentDictionary<int, object>();

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            Task consumer = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    var readTasks = new HashSet<Task>();
                    Task readTask = queue.TakeNextPartitionAsync(cts.Token)
                        .ContinueWith(t =>
                        {
                            foreach (int item in t.Result)
                            {
                                Assert.True(output.TryAdd(item, null));
                            }
                        });

                    lock (readTasks)
                    {
                        readTasks.Add(readTask);
                    }

                    _ = readTask.ContinueWith(t =>
                    {
                        lock (readTasks)
                        {
                            Assert.True(readTasks.Remove(t));
                        }
                    });

                    if (output.Count >= itemCount)
                    {
                        cts.Cancel();
                        break;
                    }

                    if (readTasks.Count >= partitionCount)
                    {
                        await Task.WhenAny(readTask);
                    }
                }
            });

            Task producer = Task.Run(() => Parallel.For(0, itemCount, options, i => queue.Add(i)));

            await Task.WhenAll(producer, consumer);

            // Make sure all expected items are present
            Assert.Equal(Enumerable.Range(0, itemCount), output.Keys.OrderBy(key => key));
        }
    }
}
