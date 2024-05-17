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

    public class TxContext : IDisposable
    {
        readonly CachePartition cachePartition;
        readonly long txId;
        readonly int partitionId;
        Phase phase;

        List<Task>? prefetchTasks;
        List<Action>? whenCompleted;
        List<Action>? whenPersisted;

        enum Phase
        {
            None,
            Prefetch,
            WaitForPrefetch,
            Execution,
            Completed,
            Aborted,
        }

        internal CachePartition CachePartition => this.cachePartition;

        public long TxId => this.txId;

        public int PartitionId => this.partitionId;

        internal TxContext(CachePartition cachePartition, long txId)
        {
            this.cachePartition = cachePartition;
            this.partitionId = cachePartition.PartitionId;
            this.txId = txId;
            this.phase = Phase.Prefetch;
        }

        public void Commit()
        {
            if (this.phase == Phase.Completed)
            {
                throw new InvalidOperationException("must not commit twice");
            }

            if (this.phase == Phase.Aborted)
            {
                throw new InvalidOperationException("transaction is already aborted");
            }

            if (this.phase == Phase.Prefetch && this.prefetchTasks != null)
            {
                throw new InvalidOperationException("must not commit before completing prefetches");
            }

            if (this.whenPersisted != null)
            {
                foreach(Action a in this.whenPersisted)
                {
                    this.cachePartition.AddPersistenceAction(a);
                }
            }

            this.phase = Phase.Completed;

            this.cachePartition.Release(this.txId, isCompletedTransaction: true);

            if (this.whenCompleted != null)
            {
                foreach (Action a in this.whenCompleted)
                {
                    a();
                }
            }
        }

        public void Abort()
        {
            if (this.phase == Phase.Aborted)
            {
                throw new InvalidOperationException("must not abort twice");
            }

            if (this.phase == Phase.Completed)
            {
                throw new InvalidOperationException("transaction is already committed");
            }

            this.cachePartition.Rollback();

            this.phase = Phase.Aborted;

            this.cachePartition.Release(this.txId, isCompletedTransaction: true);
        }

        public void Dispose()
        {
            if (this.phase < Phase.Completed)
            {
                // transaction did not commit before going out of scope - perhaps due to an unhandled exception
                // or some programmer error. Either way, we abort the transaction, to roll back all changes. 
                this.Abort();
            }
        }

        public async ValueTask CompletePrefetchesAsync()
        {
            if (this.phase > Phase.Prefetch)
            {
                throw new InvalidOperationException("cannot wait for prefetch tasks after a transaction has already exited the prefetch phase");
            }

            if (this.prefetchTasks != null)
            {
                // release the lock so other transactions can run while the prefetch is in progress

                this.phase = Phase.WaitForPrefetch;

                this.cachePartition.Release(this.txId, isCompletedTransaction: false);

                await Task.WhenAll(this.prefetchTasks).ConfigureAwait(false);

                await this.cachePartition.ContinueTransactionAsync(this.txId).ConfigureAwait(false);
            }

            this.EnterExecutionPhase();
        }

        internal void EnterExecutionPhase()
        {
            this.phase = Phase.Execution;
        }

        internal void EnsureExecutionPhase()
        {
            switch (this.phase)
            {
                case Phase.Prefetch:
                    if (this.prefetchTasks != null)
                    {
                        throw new InvalidOperationException($"must call {nameof(CompletePrefetchesAsync)} first");
                    }
                    this.EnterExecutionPhase(); // we move forward into execution phase implicitly
                    return;
                case Phase.WaitForPrefetch:
                    throw new InvalidOperationException($"transaction is waiting for prefetches to complete");
                case Phase.Execution:
                    return;
                case Phase.Completed:
                    throw new InvalidOperationException("transaction has already completed execution");
            }
        }

        internal void EnsurePrefetchPhase()
        {
            switch (this.phase)
            {
                case Phase.Prefetch:
                    return;
                case Phase.WaitForPrefetch:
                    throw new InvalidOperationException($"transaction is waiting for prefetches to complete");
                case Phase.Execution:
                    throw new InvalidOperationException("cannot prefetch after entering execution phase");
                case Phase.Completed:
                    throw new InvalidOperationException("transaction has already completed execution");
            }
        }

        public long GetNextSequenceNumber()
        {
            this.EnsureExecutionPhase();
            return this.cachePartition.GetNextSequenceNumber();
        }

        public void WhenCompleted(Action action)
        {
            if (this.phase > Phase.Execution)
            {
                throw new InvalidOperationException("cannot register a completion action after a transaction has already completed execution");
            }
            (this.whenCompleted ??= new List<Action>()).Add(action);
        }

        public void WhenPersisted(Action action)
        {
            if (this.phase > Phase.Execution)
            {
                throw new InvalidOperationException("cannot register a persistence action after a transaction has already completed execution");
            }
            (this.whenPersisted ??= new List<Action>()).Add(action);
        }

        internal void AddPrefetchTask(Task task)
        {
            if (this.phase > Phase.Prefetch)
            {
                throw new InvalidOperationException("cannot register a prefetch task after a transaction has already exited the prefetch phase");
            }
            (this.prefetchTasks ??= new List<Task>()).Add(task);
        }

    }
}
