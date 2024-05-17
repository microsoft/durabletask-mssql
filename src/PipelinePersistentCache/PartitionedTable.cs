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
        }

        // subclasses override this to load the current value of a row from storage
        protected abstract Task<(bool exists, TValue? value)> LoadRowAsync(TKey key);

        // subclasses override this to create deltas for writing back to storage.
        protected abstract void AddRowDeltaToCheckpointCommand(TCommand command, Writeback writeback, int partitionId, TKey key, TValue? Current);

        protected void PrefetchRow(TxContext tx, TKey key)
        {
            tx.EnsurePrefetchPhase();

            if (this.rows.TryGetValue(key, out var info))
            {
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

            tx.WhenCompleted(() => info.RefCount--);

            if (info.Pending != null)
            {
                tx.AddPrefetchTask(info.Pending);
            }
        }

        protected void CreateNonExistingRow(TxContext tx, TKey key, TValue value)  // this is the only operation that can be used without prefetching
        {
            tx.EnsureExecutionPhase();

            if (this.rows.TryGetValue(key, out var info))
            {
                if (info.Pending?.IsCompleted == false)
                {
                    throw new InvalidOperationException("Table.Create must wait for entry to be fetched from storage");
                }
                if (info.Exists)
                {
                    throw new InvalidOperationException("Table.Create must not be called for an already existing row");
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
        }

        protected bool TryGetRow(TxContext tx, TKey key, out TValue? value)
        {
            tx.EnsureExecutionPhase();

            if (!this.rows.TryGetValue(key, out var info))
            {
                throw new InvalidOperationException("Table.TryGetValue must only be used for in-memory entries. Did you forget to prefetch?");
            }
            else
            {
                if (info.Pending?.IsCompleted == false)
                {
                    throw new InvalidOperationException("Table.TryGetValue must wait for entry to be fetched from storage");
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
                throw new InvalidOperationException("Table.Update must update an existing row. Did you forget to prefetch?");
            }
            if (info.Pending?.IsCompleted == false)
            {
                throw new InvalidOperationException("Table.Update must wait for entry to be fetched from storage");
            }
            if (!info.Exists)
            {
                throw new InvalidOperationException("Table.Update must update an existing row");
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

            if (!this.rows.TryGetValue(key, out var info) || !info.Exists)
            {
                throw new InvalidOperationException("Table.Delete must delete an existing row. Did you forget to prefetch?");
            }
            if (info.Pending?.IsCompleted == false)
            {
                throw new InvalidOperationException("Table.Delete must wait for entry to be fetched from storage");
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
