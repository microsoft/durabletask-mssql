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
        readonly HashSet<Tracked> Writebacks;
        readonly List<Action> postCheckpointActions = new List<Action>();

        readonly SemaphoreSlim executionLock;  // we probably want to revise this at some point since semaphores are not fair
        long lockHolder;

        long LastSequenceNumber;
  
        public PipelinePersistentCache()
        {
            this.Writebacks = new();
            this.postCheckpointActions = new();
            this.executionLock = new(1);
            this.LastSequenceNumber = 0;
        }

        /// <summary>
        /// Fires whenever a transaction completes. Can be usefult to trigger checkpoints.
        /// </summary>
        public event Action<long>? OnTransactionCompleted;


        public async Task<TxContext> StartTransactionAsync(CancellationToken token)
        {
            await this.executionLock.WaitAsync(token);
            long txId = this.GetNextSequenceNumber();
            this.lockHolder = txId;   
            return new TxContext(this, txId); 
        }

        internal async ValueTask ReAcquireAsync(long txId)
        {
            await this.executionLock.WaitAsync();
            this.lockHolder = txId;
        }

        internal void Release(long txId, bool notify = false)
        {
            Debug.Assert(this.lockHolder == txId);
            this.executionLock.Release();

            if (notify && this.OnTransactionCompleted != null)
            {
                this.OnTransactionCompleted(txId);
            }
        }

        public async Task CollectNextCheckpointAsync<TCommand>(TCommand command)
            where TCommand : CheckpointCommand
        {
            await this.executionLock.WaitAsync(); // we must collect deltas under the lock so see a consistent state

            //  TODO make more robust if command implementation should throw

            // persist the latest value of the sequence counter, incremented by one
            // so it simultaneously serves as an ID for this checkpoint
            command.SetSequenceCounter(this.GetNextSequenceNumber());

            // collect and clear all the deltas
            foreach (var tracked in this.Writebacks)
            {
                tracked.AddWriteback(command);
            }
            this.Writebacks.Clear();

            // collect and clear all the post-checkpoint actions
            command.AddPostCheckpointActions(this.postCheckpointActions);
            this.postCheckpointActions.Clear();

            this.executionLock.Release();
        }

        internal long GetNextSequenceNumber()
        {
            return ++this.LastSequenceNumber;
        }

        internal void AddPersistenceActions(IEnumerable<Action> action)
        {
            this.postCheckpointActions.AddRange(action);
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

        internal abstract class Tracked
        {
            public abstract void AddWriteback(object command);
        }
    }
}
