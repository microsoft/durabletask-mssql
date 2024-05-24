// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Transactions;

    /// <summary>
    /// A cache that is backed by storage and supports transactions and pipelined persistence.
    /// </summary>
    public class PipelinePersistentCache
    {
        readonly CachePartition?[] cachePartitions;
        readonly PartitionedTable[] tables;

        /// <summary>
        /// Constructs a pipeline persistent cache with the given number of partitions.
        /// </summary>
        /// <param name="partitionCount"></param>
        public PipelinePersistentCache(int partitionCount, IEnumerable<PartitionedTable> tables)
        {
            this.cachePartitions = new CachePartition[partitionCount];
        }
         
        public delegate void TransactionCompleted(long txId);

        public int TotalPartitions => this.cachePartitions.Length;

        /// <summary>
        /// Add the given partition to the set of partitions owned by this cache.
        /// </summary>
        /// <param name="partitionMetaData"></param>
        public async Task ActivatePartitionAsync(PartitionMetaData partitionMetaData)
        {
            if (this.cachePartitions[partitionMetaData.PartitionId] != null)
            {
                throw new InvalidOperationException("Partition already activated");
            }   

            var cachePartition = new CachePartition(this, partitionMetaData);

            ConcurrentBag<TrackedRow> rowRecoveredNotifications = new ConcurrentBag<TrackedRow>();

            await Parallel.ForEachAsync(this.tables, (table, cancellation) => table.RecoverRowsAsync(partitionMetaData.PartitionId, rowRecoveredNotifications));

            // add the partition to the array. From this point on, it will be included in checkpoint operations.
            this.cachePartitions[partitionMetaData.PartitionId] = cachePartition;

            // run the row recovered notifications, which will restart tasks that were in progress when the partition shut down.
            foreach (var row in rowRecoveredNotifications)
            {
                row.NotifyRecovered();
            }
        }

        /// <summary>
        /// Removes the given partition from the set of partitions owned by this cache.
        /// </summary>
        /// <param name="partitionMetaData"></param>
        public void RemovePartition(int partitionId)
        {
            if (this.cachePartitions[partitionId] == null)
            {
                throw new InvalidOperationException("Partition not owned by cache");
            }
            this.cachePartitions[partitionId] = null;
        }

        public IEnumerable<int> OwnedPartitions => this.cachePartitions
            .Select((partition, index) => partition != null ? index : -1)
            .Where(index => index != -1);

        CachePartition GetPartition(int partitionId)
        {
            var cachePartition = this.cachePartitions[partitionId];
            if (cachePartition == null)
            {
                throw new ArgumentOutOfRangeException(nameof(partitionId), "partition is not owned by this cache");
            }
            return cachePartition;
        }

        public void AddTransactionCompletionListener(int partitionId, TransactionCompleted listener)
        {
            this.GetPartition(partitionId).OnTransactionCompleted += listener;
        }
        public void RemoveTransactionCompletionListener(int partitionId, TransactionCompleted listener)
        {
            this.GetPartition(partitionId).OnTransactionCompleted -= listener;
        }

        public ValueTask<TxContext> StartTransactionAsync(int partitionId)
        {
            return this.GetPartition(partitionId).StartTransactionAsync();
        }

        public Task CollectNextCheckpointAsync<TCommand>(TCommand command)
            where TCommand : CheckpointCommand
        {
            return Parallel.ForEachAsync(this.cachePartitions, (partition, cancellation) => partition?.CollectNextCheckpointAsync(command, cancellation) ?? default);
        }

        public void EvictLeastRecentlyUsed()
        {
            //TODO
        }
    }
}
