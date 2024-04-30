// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class TestTable : Table<string, string, TestCheckpoint>
    {
        readonly string name;
        readonly Func<string, Task<(bool exists, string? value)>> load;

        public TestTable(string name, Func<string, Task<(bool exists, string? value)>> load)
        {
            this.name = name;
            this.load = load;
        }

        protected override void AddDeltaToCheckpointCommand(TestCheckpoint command, Writeback writeback, string key, string? Current)
        {
            command.AddDelta(this.name, key, Current, writeback);
        }

        protected override Task<(bool exists, string? value)> LoadAsync(string key)
        {
            return this.load(key);
        }

        new public void Prefetch(TxContext tx, string key)
        {
            base.Prefetch(tx, key);
        }

        new public void Create(TxContext tx, string key, string value)  // this is the only operation that can be used without prefetching
        {
            base.Create(tx, key, value);
        }

        new public bool TryGetValue(TxContext tx, string key, out string? value)
        {
            return base.TryGetValue(tx, key, out value);
        }

        new public void Update(TxContext tx, string key, string value)
        {
            base.Update(tx, key, value);
        }

        new public void Delete(TxContext tx, string key)
        {
            base.Delete(tx, key);
        }
    }
}
