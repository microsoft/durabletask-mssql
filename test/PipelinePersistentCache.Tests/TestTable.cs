// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Threading.Tasks;

    class TestTable : PartitionedTable<string, string, TestCheckpoint>
    {
        readonly string name;
        readonly Func<string, Task<(bool exists, string? value)>> load;

        public TestTable(string name, Func<string, Task<(bool exists, string? value)>> load)
        {
            this.name = name;
            this.load = load;
        }

        protected override void AddRowDeltaToCheckpointCommand(TestCheckpoint command, Writeback writeback, int partitionId, string key, string? Current)
        {
            command.AddDelta(this.name, partitionId, key, Current, writeback);
        }

        protected override Task<(bool exists, string? value)> LoadRowAsync(string key)
        {
            return this.load(key);
        }

        new public void PrefetchRow(TxContext tx, string key)
        {
            base.PrefetchRow(tx, key);
        }

        new public void CreateNonExistingRow(TxContext tx, string key, string value)  // this is the only operation that can be used without prefetching
        {
            base.CreateNonExistingRow(tx, key, value);
        }

        new public bool TryGetRow(TxContext tx, string key, out string? value)
        {
            return base.TryGetRow(tx, key, out value);
        }

        new public void UpdateExistingRow(TxContext tx, string key, string value)
        {
            base.UpdateExistingRow(tx, key, value);
        }

        new public void DeleteExistingRow(TxContext tx, string key)
        {
            base.DeleteExistingRow(tx, key);
        }
    }
}
