// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    using System;
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

        /// <summary>
        /// Constructs a pipeline persistent cache with one statically owned partition.
        /// </summary>
        public PipelinePersistentCache() : this(1)
        {
            this.AddPartition(PartitionMetaData.GetInitialState(0, 1));
        }

        /// <summary>
        /// Constructs a pipeline persistent cache with the given number of dynamically owned partitions.
        /// </summary>
        /// <param name="totalPartitions"></param>
        public PipelinePersistentCache(int totalPartitions)
        {
            this.cachePartitions = new CachePartition[totalPartitions];
        }
         
        public delegate void TransactionCompleted(long txId);

        public int TotalPartitions => this.cachePartitions.Length;

        /// <summary>
        /// Add the given partition to the set of partitions owned by this cache.
        /// </summary>
        /// <param name="partitionMetaData"></param>
        public void AddPartition(PartitionMetaData partitionMetaData)
        {
            if (this.cachePartitions[partitionMetaData.PartitionId] != null)
            {
                throw new InvalidOperationException("Partition already owned by cache");
            }   
            this.cachePartitions[partitionMetaData.PartitionId] = new CachePartition(this, partitionMetaData);
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
