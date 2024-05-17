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
        readonly HashSet<TrackedRow> writebacks; // the rows that were modified since the last checkpoint
        readonly List<TrackedRow> rollbacks; // the rows that were modified by the current transaction
        readonly List<Action> postCheckpointActions;
        readonly long[] deduplicationVector;
        readonly object partitionLockLock; // protects the following fields
        long? partitionLockHolder;  // the ID of the transaction or checkpoint that currently holds the global partition lock, or null if not held
        Queue<TaskCompletionSource>? partitionLockWaiters;  // the queue of tasks waiting for the global partition lock
        long lastSequenceNumber; // a counter for monotonically increasing transaction and checkpoint IDs

        public event PipelinePersistentCache.TransactionCompleted? OnTransactionCompleted;

        public CachePartition(PipelinePersistentCache cache, PartitionMetaData partitionMetaData)
        {
            this.partitionId = partitionMetaData.PartitionId;
            this.cache = cache;
            this.writebacks = new();
            this.rollbacks = new();
            this.postCheckpointActions = new();
            this.deduplicationVector = partitionMetaData.DeduplicationVector;
            this.partitionLockLock = new();
            this.partitionLockHolder = null;
            this.partitionLockWaiters = null; // we use lazy allocation because the queue may never be needed
            this.lastSequenceNumber = 0;
        }

        public int PartitionId => this.partitionId;

        public long GetNextSequenceNumber()
        {
            return ++this.lastSequenceNumber;
        }

        public ValueTask<TxContext> StartTransactionAsync()
        {
            long transactionId;

            lock (this.partitionLockLock)
            {
                if (this.partitionLockHolder == null)
                {
                    // fast path: transaction lock is available and we can immediately start the transaction.
                    transactionId = this.GetNextSequenceNumber();
                    this.partitionLockHolder = transactionId;
                    return new ValueTask<TxContext>(new TxContext(this, transactionId));
                }
                else
                {
                    return SlowPath();
                }
            }

            async ValueTask<TxContext> SlowPath()
            {
                // the transaction lock is currently held, so in order to wait for it, we add ourselves to the queue of waiters.
                var tcs = new TaskCompletionSource();
                (this.partitionLockWaiters ??= new()).Enqueue(tcs);
                await tcs.Task.ConfigureAwait(false);
                // we now have the transaction lock
                transactionId = this.GetNextSequenceNumber();
                this.partitionLockHolder = transactionId;
                return new TxContext(this, transactionId);
            }
        }

        public ValueTask ContinueTransactionAsync(long transactionId)
        {
            lock (this.partitionLockLock)
            {
                if (this.partitionLockHolder == null)
                {
                    // fast path: transaction lock is available and we can immediately start the transaction.
                    this.partitionLockHolder = transactionId;
                    return default;
                }
                else
                {
                    return SlowPath();
                }
            }

            async ValueTask SlowPath()
            {
                // the transaction lock is currently held, so in order to wait for it, we add ourselves to the queue of waiters.
                var tcs = new TaskCompletionSource();
                (this.partitionLockWaiters ??= new()).Enqueue(tcs);
                await tcs.Task.ConfigureAwait(false);
                this.partitionLockHolder = transactionId;
            }
        }

        ValueTask<long> StartCheckpointAsync()
        {
            long checkpointId;

            lock (this.partitionLockLock)
            {
                if (this.partitionLockHolder == null)
                {
                    // fast path: transaction lock is available and we can immediately start the transaction.
                    checkpointId = this.GetNextSequenceNumber();
                    this.partitionLockHolder = checkpointId;
                    return new ValueTask<long>(checkpointId);
                }
                else
                {
                    return SlowPath();
                }
            }

            async ValueTask<long> SlowPath()
            {
                // the transaction lock is currently held, so in order to wait for it, we add ourselves to the queue of waiters.
                var tcs = new TaskCompletionSource();
                (this.partitionLockWaiters ??= new()).Enqueue(tcs);
                await tcs.Task.ConfigureAwait(false);
                checkpointId = this.GetNextSequenceNumber();
                this.partitionLockHolder = checkpointId;
                return checkpointId;
            }
        }

        public void Release(long id, bool isCompletedTransaction)
        {
            TaskCompletionSource? next = null;

            if (isCompletedTransaction)
            {
                foreach(var row in this.rollbacks)
                {
                    row.DiscardRollbackInformation();
                }

                this.rollbacks.Clear();
            }

            lock (this.partitionLockLock)
            {
                Debug.Assert(this.partitionLockHolder == id);

                this.partitionLockHolder = null;

                if (this.partitionLockWaiters != null)
                {
                    this.partitionLockWaiters.TryDequeue(out next);
                }
            }

            if (isCompletedTransaction && this.OnTransactionCompleted != null)
            {
                this.OnTransactionCompleted(id);
            }

            if (next != null)
            {
                next.SetResult(); // we do this outside of the lock, and after the notification, since it may execute synchronously
            }
        }

        public void AddRollback(TrackedRow row)
        {
            this.rollbacks.Add(row);
        }

        public void Rollback()
        {
            foreach (var row in this.rollbacks)
            {
                row.Rollback();
            }
        }

        internal void AddWriteback(TrackedRow tracked)
        {
            bool success = this.writebacks.Add(tracked);
            Debug.Assert(success);
        }

        internal void RemoveWriteback(TrackedRow tracked)
        {
            bool success = this.writebacks.Remove(tracked);
            Debug.Assert(success);
        }

        internal void AddPersistenceAction(Action action)
        {
            this.postCheckpointActions.Add(action);
        }

      
        public async ValueTask CollectNextCheckpointAsync<TCommand>(TCommand command, CancellationToken cancellation)
           where TCommand : CheckpointCommand
        {
            // acquire the partition lock and get a checkpoint id
            long checkpointId = await this.StartCheckpointAsync().ConfigureAwait(false); 

            try
            {
                // persist the latest value of the sequence counter, incremented by one
                // so it simultaneously serves as an ID for this checkpoint
                var metadata = new PartitionMetaData(this.partitionId, checkpointId, this.deduplicationVector.ToArray());
                lock (command)
                {
                    command.SetPartitionMetaData(metadata);
                }

                // collect and clear all the deltas
                foreach (var tracked in this.writebacks)
                {
                    tracked.AddChangesToCheckpointCommand(command);
                }
                this.writebacks.Clear();

                // collect and clear all the post-checkpoint actions
                lock (command)
                {
                    foreach(Action a in this.postCheckpointActions)
                    {
                        command.AddPostCheckpointAction(a);
                    }   
                }
                this.postCheckpointActions.Clear();
            }
            finally
            {
                this.Release(checkpointId, isCompletedTransaction: false);
            }
        }
    }
}
