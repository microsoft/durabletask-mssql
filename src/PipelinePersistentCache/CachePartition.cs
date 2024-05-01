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

    class CachePartition
    {
        readonly int partitionId;
        readonly PipelinePersistentCache cache;
        readonly HashSet<Tracked> Writebacks;
        readonly List<Action> postCheckpointActions;
        readonly long[] deduplicationVector;

        readonly SemaphoreSlim executionLock;  // we probably want to revise this at some point since semaphores are not fair
        long? lockHolder;

        long LastSequenceNumber;

        public event PipelinePersistentCache.TransactionCompleted OnTransactionCompleted;

        public CachePartition(PipelinePersistentCache cache, PartitionMetaData partitionMetaData)
        {
            this.partitionId = partitionMetaData.PartitionId;
            this.cache = cache;
            this.Writebacks = new();
            this.postCheckpointActions = new();
            this.deduplicationVector = partitionMetaData.DeduplicationVector;

            this.executionLock = new(1);
            this.lockHolder = null;
            this.LastSequenceNumber = 0;
        }

        public int PartitionId => this.partitionId;

        public long GetNextSequenceNumber()
        {
            return ++this.LastSequenceNumber;
        }

        public async Task<TxContext> StartTransactionAsync(CancellationToken token)
        {
            await this.executionLock.WaitAsync(token);
            long txId = this.GetNextSequenceNumber();
            this.lockHolder = txId;
            return new TxContext(this, txId);
        }

        public async ValueTask ReAcquireAsync(long txId)
        {
            await this.executionLock.WaitAsync();
            this.lockHolder = txId;
        }

        public void Release(long txId, bool notify = false)
        {
            Debug.Assert(this.lockHolder == txId);
            this.executionLock.Release();

            if (notify && this.OnTransactionCompleted != null)
            {
                this.OnTransactionCompleted(txId);
            }
        }

        internal void AddWriteback(Tracked tracked)
        {
            bool success = this.Writebacks.Add(tracked);
            Debug.Assert(success);
        }

        internal void RemoveWriteback(Tracked tracked)
        {
            bool success = this.Writebacks.Remove(tracked);
            Debug.Assert(success);
        }

        internal void AddPersistenceActions(IEnumerable<Action> action)
        {
            this.postCheckpointActions.AddRange(action);
        }

        internal abstract class Tracked
        {
            public abstract void AddWriteback(object command);
        }


        public async Task CollectNextCheckpointAsync<TCommand>(TCommand command)
           where TCommand : CheckpointCommand
        {
            await this.executionLock.WaitAsync(); // we must collect deltas under the lock so see a consistent state

            //  TODO make more robust if command implementation should throw

            // persist the latest value of the sequence counter, incremented by one
            // so it simultaneously serves as an ID for this checkpoint
            var metadata = new PartitionMetaData(this.partitionId, this.GetNextSequenceNumber(), this.deduplicationVector.ToArray());
            lock (command)
            {
                command.SetPartitionMetaData(metadata);
            }

            // collect and clear all the deltas
            foreach (var tracked in this.Writebacks)
            {
                tracked.AddWriteback(command);
            }
            this.Writebacks.Clear();

            // collect and clear all the post-checkpoint actions
            lock (command)
            {
                command.AddPostCheckpointActions(this.postCheckpointActions);
            }
            this.postCheckpointActions.Clear();

            this.executionLock.Release();
        }

    }
}
