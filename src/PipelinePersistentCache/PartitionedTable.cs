// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Reflection.Metadata.Ecma335;
    using System.Runtime.InteropServices.ObjectiveC;
    using System.Text;
    using System.Threading.Tasks;


    /// <summary>
    /// A partitioned table that stores rows that can be created, deleted, and updated within transactions.
    /// </summary>
    public abstract class PartitionedTable
    {
        // called when a partition is being activated
        internal abstract ValueTask RecoverRowsAsync(int partitionId, ConcurrentBag<TrackedRow> rowRecoveredNotifications);
    }

    /// <summary>
    /// A partitioned table with specific types for keys, values, and checkpoint commands.
    /// </summary>
    /// <typeparam name="TKey">The type representing all the primary key columns.</typeparam>
    /// <typeparam name="TValue">The type representing all other columns.</typeparam>
    /// <typeparam name="TCommand">The type of the checkpoint command for writing back changes to the store.</typeparam>
    public abstract class PartitionedTable<TKey, TValue, TCommand> : PartitionedTable
        where TKey : notnull
        where TCommand : CheckpointCommand
    {
        readonly Dictionary<TKey, RowInfo> rows;

        protected PartitionedTable()
        {
            this.rows = new();
        }

        internal class RowInfo : TrackedRow
        {
            readonly PartitionedTable<TKey, TValue, TCommand> table;
            readonly int partitionId;
            readonly TKey key;

            RollbackInfo rollbackInfo;

            public RowInfo(PartitionedTable<TKey, TValue, TCommand> table, int partitionId, TKey key)
            {
                this.table = table;
                this.partitionId = partitionId;
                this.key = key;
            }
            public RowInfo(PartitionedTable<TKey, TValue, TCommand> table, int partitionId, TKey key, TValue value)
                :this(table, partitionId, key)
            {
                this.Current = value;
                this.Exists = true;
            }

            public TValue? Current;
            public bool Exists;
            public Writeback Writeback;

            public long LastUse;
            public int RefCount;
            public Task? Pending;


            public override void AddChangesToCheckpointCommand(object command)
            {
                if (this.Writeback != Writeback.None)
                {
                    lock (command)
                    {
                        this.table.AddRowDeltaToCheckpointCommand((TCommand)command, this.Writeback, this.partitionId, this.key, this.Current);
                    }
                }
            }

            public struct RollbackInfo
            {
                public bool HasRollbackInfo;
                public TValue? Current;
                public bool Exists;
                public Writeback Writeback;
            }

            public void CaptureRollbackInformation(CachePartition cachePartition)
            {
                if (!this.rollbackInfo.HasRollbackInfo)
                {
                    this.rollbackInfo.HasRollbackInfo = true;
                    this.rollbackInfo.Writeback = this.Writeback;
                    this.rollbackInfo.Current = this.Current;
                    this.rollbackInfo.Exists = this.Exists;
                    cachePartition.AddRollback(this);
                }
            }

            public override void DiscardRollbackInformation()
            {
                this.rollbackInfo = default; 
            }

            public override void Rollback()
            {
                if (this.rollbackInfo.HasRollbackInfo)
                {
                    this.Writeback = this.rollbackInfo.Writeback;
                    this.Current = this.rollbackInfo.Current;
                    this.Exists = this.rollbackInfo.Exists;
                }
            }

            public override void DecrementReferenceCount()
            {
                this.RefCount--;
            }

            public override void NotifyRecovered()
            {
                this.table.OnNewOrRecoveredRow?.Invoke(this.key, this.Current!);
            }
        }

        // subclasses override this to load the current value of a row from storage
        protected abstract Task<(bool exists, TValue? value)> LoadRowAsync(TKey key);

        // subclasses override this to create deltas for writing back to storage.
        protected abstract void AddRowDeltaToCheckpointCommand(TCommand command, Writeback writeback, int partitionId, TKey key, TValue? Current);

        // called while a partition is being activated, to recover rows that need to be in memory before the partition starts
        protected abstract IAsyncEnumerable<(TKey, TValue)> RecoverRowsAsync(int partitionId);

        // an event that is raised when a new row is created or an existing row is recovered
        public event RowListener? OnNewOrRecoveredRow;

        public delegate void RowListener(TKey key, TValue value);

        internal override async ValueTask RecoverRowsAsync(int partitionId, ConcurrentBag<TrackedRow> rowRecoveredNotifications)
        {
            await foreach ((TKey key, TValue value) in this.RecoverRowsAsync(partitionId))
            {
                var rowInfo = new RowInfo(this, partitionId, key, value);
                this.rows.Add(key, rowInfo);
                if (this.OnNewOrRecoveredRow != null)
                {
                    rowRecoveredNotifications.Add(rowInfo);
                }
            }
        }

        /// <summary>
        /// Ensures that the row information for the given key is present in memory. If the row exists, its value is also stored in memory.
        /// If the row does not exist, a placeholder is created in memory, and the row is marked as non-existent.
        /// </summary>
        /// <param name="tx">The transaction context.</param>
        /// <param name="key">The row key.</param>
        protected void EnsureInMemory(TxContext tx, TKey key)
        {
            tx.EnsurePrefetchPhase();

            if (this.rows.TryGetValue(key, out var info))
            {
                // we don't have to prefetch, but we still have to increment the ref count
                // to ensure the row stays in memory until the transaction is done.
                info.RefCount++;
            }
            else
            {
                info = new RowInfo(this, tx.PartitionId, key) { RefCount = 1 };

                info.Pending = Task.Run(async () =>
                {
                    (info.Exists, info.Current) = await this.LoadRowAsync(key).ConfigureAwait(false); 
                    info.Writeback = Writeback.None;
                });

                this.rows.Add(key, info);
            }

            tx.AddPrefetchedRow(info);

            if (info.Pending != null)
            {
                tx.AddPrefetchTask(info.Pending);
            }
        }

        protected void CreateFreshRow(TxContext tx, TKey key, TValue value)  // this is the only operation that can be used without prefetching
        {
            tx.EnsureExecutionPhase();

            if (this.rows.TryGetValue(key, out var info))
            {
                if (info.Pending?.IsCompleted == false)
                {
                    throw new InvalidOperationException("Must wait for prefetch to complete.");
                }
                if (info.Exists)
                {
                    throw new InvalidOperationException("Row already exists.");
                }
            }
            else
            {
                this.rows.Add(key, info = new RowInfo(this, tx.PartitionId, key));
            }

            info.CaptureRollbackInformation(tx.CachePartition);

            info.Current = value;
            info.Exists = true;

            if (info.Writeback == Writeback.Deleted)
            {
                info.Writeback = Writeback.Updated;
            }
            else
            {
                if (info.Writeback == Writeback.None)
                {
                    tx.CachePartition.AddWriteback(info);
                }
                info.Writeback = Writeback.Created;
            }

            info.LastUse = tx.TxId;

            if (this.OnNewOrRecoveredRow != null)
            {
                tx.WhenCompleted(() => this.OnNewOrRecoveredRow(key, value));
            }     
        }

        protected bool TryGetRow(TxContext tx, TKey key, out TValue? value)
        {
            tx.EnsureExecutionPhase();

            if (!this.rows.TryGetValue(key, out var info))
            {
                throw new InvalidOperationException("Row information not found in memory. Did you forget to prefetch?");
            }
            else
            {
                if (info.Pending?.IsCompleted == false)
                {
                    throw new InvalidOperationException("Must wait for prefetch to complete.");
                }
                info.LastUse = tx.TxId;
                value = info.Current;
                return info.Exists;
            }
        }

        protected void UpdateExistingRow(TxContext tx, TKey key, TValue value)
        {
            tx.EnsureExecutionPhase();

            if (!this.rows.TryGetValue(key, out var info))
            {
                throw new InvalidOperationException("Row information not found in memory. Did you forget to prefetch?");
            }
            if (info.Pending?.IsCompleted == false)
            {
                throw new InvalidOperationException("Must wait for prefetch to complete.");
            }
            if (!info.Exists)
            {
                throw new InvalidOperationException("Row does not exist.");
            }

            info.CaptureRollbackInformation(tx.CachePartition);

            info.Current = value;
            
            if (info.Writeback == Writeback.Created)
            {
                info.Writeback = Writeback.Created;
            }
            else
            {
                if (info.Writeback == Writeback.None)
                {
                    tx.CachePartition.AddWriteback(info);
                }

                info.Writeback = Writeback.Updated;
            }

            info.LastUse = tx.TxId;
        }

        protected void DeleteExistingRow(TxContext tx, TKey key)
        {
            tx.EnsureExecutionPhase();

            if (!this.rows.TryGetValue(key, out var info))
            {
                throw new InvalidOperationException("Row information not found in memory. Did you forget to prefetch?");
            }
            if (info.Pending?.IsCompleted == false)
            {
                throw new InvalidOperationException("Must wait for row information to finish loading from storage.");
            }
            if (!info.Exists)
            {
                throw new InvalidOperationException("Row does not exist.");
            }

            info.CaptureRollbackInformation(tx.CachePartition);

            info.Current = default;
            info.Exists = false;

            if (info.Writeback == Writeback.Created)
            {
                info.Writeback = Writeback.None;
                tx.CachePartition.RemoveWriteback(info);
            }
            else 
            {
                if (info.Writeback == Writeback.None)
                {
                    tx.CachePartition.AddWriteback(info);
                }

                info.Writeback = Writeback.Deleted;
            }

            info.LastUse = tx.TxId;
        }
    }
}
