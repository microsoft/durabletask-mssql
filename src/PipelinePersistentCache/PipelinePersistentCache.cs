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
        readonly CachePartition[] cachePartitions;

        public PipelinePersistentCache(int totalPartitions, IEnumerable<PartitionMetaData> ownedPartitions)
        {
            this.cachePartitions = new CachePartition[totalPartitions];

            foreach (var partitionMetaData in ownedPartitions)
            {
                this.cachePartitions[partitionMetaData.PartitionId] = new CachePartition(this, partitionMetaData);
            }
        }

        public PipelinePersistentCache(int totalPartitions = 1)
        {
            this.cachePartitions = new CachePartition[totalPartitions];

            for (int i = 0; i < totalPartitions; i++)
            {
                this.cachePartitions[i] = new CachePartition(this, PartitionMetaData.GetInitialState(i, totalPartitions));
            }
        }
         
        public delegate void TransactionCompleted(long txId);

        public int TotalPartitions => this.cachePartitions.Length;

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
