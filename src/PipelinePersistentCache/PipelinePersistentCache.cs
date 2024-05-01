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
        readonly CachePartition[] txManagers;

        public PipelinePersistentCache(int totalPartitions, IEnumerable<PartitionMetaData> ownedPartitions)
        {
            this.txManagers = new CachePartition[totalPartitions];

            foreach (var partitionMetaData in ownedPartitions)
            {
                this.txManagers[partitionMetaData.PartitionId] = new CachePartition(this, partitionMetaData);
            }
        }

        public PipelinePersistentCache(int totalPartitions = 1)
        {
            this.txManagers = new CachePartition[totalPartitions];

            for (int i = 0; i < totalPartitions; i++)
            {
                this.txManagers[i] = new CachePartition(this, PartitionMetaData.GetInitialState(i, totalPartitions));
            }
        }
         
        public delegate void TransactionCompleted(long txId);

        public void AddTransactionCompletionListener(int partitionId, TransactionCompleted listener)
        {
            this.txManagers[partitionId].OnTransactionCompleted += listener;
        }
        public void RemoveTransactionCompletionListener(int partitionId, TransactionCompleted listener)
        {
            this.txManagers[partitionId].OnTransactionCompleted -= listener;
        }

        public Task<TxContext> StartTransactionAsync(int partitionId, CancellationToken token)
        {
            return this.txManagers[partitionId].StartTransactionAsync(token);
        }

        public Task CollectNextCheckpointAsync<TCommand>(TCommand command, int partitionId = 0)
            where TCommand : CheckpointCommand
        {
            // TODO once we shortcut cross-partition communcation we can no longer checkpoint partitions individually

            var txManager = this.txManagers[partitionId];
            return txManager.CollectNextCheckpointAsync(command);
        }

        public void EvictLeastRecentlyUsed()
        {
            //TODO
        }
    }
}
