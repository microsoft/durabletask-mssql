// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public  class ReductionTests
    {
        [Fact]
        public async Task CreateUpdateDelete()
        {
            var cache = new PipelinePersistentCache();
            var table = new TestTable("A", (key) =>
            {
                Assert.Fail("no load expected for this test");
                return default;
            });

            TxContext tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 1);
            table.CreateNonExistingRow(tx, "a", "1");
            table.CreateNonExistingRow(tx, "b", "2");
            table.CreateNonExistingRow(tx, "c", "3");
            table.CreateNonExistingRow(tx, "d", "4");
            table.DeleteExistingRow(tx, "b");
            table.UpdateExistingRow(tx, "c", "33");
            table.UpdateExistingRow(tx, "d", "44");
            tx.Commit();


            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 2);
            Assert.True(table.TryGetRow(tx, "a", out var value)); Assert.Equal("1", value);
            Assert.False(table.TryGetRow(tx, "b", out value));
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("33", value);
            Assert.True(table.TryGetRow(tx, "d", out value)); Assert.Equal("44", value);
            tx.Commit();

            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 3);
            table.UpdateExistingRow(tx, "d", "444");
            tx.Commit();

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);

            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 5);
            table.UpdateExistingRow(tx, "d", "4444");
            tx.Commit();

            Assert.Equal(4, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);

            Assert.Collection(checkpoint.GetDeltas("A", 0),
                delta => Assert.Equal(("a", "1", Writeback.Created), delta),
                delta => Assert.Equal(("c", "33", Writeback.Created), delta),
                delta => Assert.Equal(("d", "444", Writeback.Created), delta));

            tx = await cache.StartTransactionAsync(0);
            Assert.True(table.TryGetRow(tx, "a", out value)); Assert.Equal("1", value);
            Assert.False(table.TryGetRow(tx, "b", out value));
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("33", value);
            Assert.True(table.TryGetRow(tx, "d", out value)); Assert.Equal("4444", value);
            tx.Commit();
        }

        [Fact]
        public async Task ReadUpdateDelete()
        {
            var cache = new PipelinePersistentCache();

            var table = new TestTable("A", async (key) => {
                await Task.Delay(TimeSpan.FromMicroseconds(10));
                switch(key)
                {
                    case "a": return (true, "1");
                    case "b": return (true, "2");
                    case "c": return (true, "3");
                    case "d": return (true, "4");
                    default: return (false, null);
                }
            });

            TxContext tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 1);
            table.EnsureInMemory(tx, "a");
            table.EnsureInMemory(tx, "b");
            table.EnsureInMemory(tx, "c");
            table.EnsureInMemory(tx, "d");
            table.EnsureInMemory(tx, "e");
            await tx.CompletePrefetchesAsync();
            Assert.True(table.TryGetRow(tx, "a", out var value)); Assert.Equal("1", value);
            Assert.True(table.TryGetRow(tx, "b", out value)); Assert.Equal("2", value);
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("3", value);
            Assert.True(table.TryGetRow(tx, "d", out value)); Assert.Equal("4", value);
            Assert.False(table.TryGetRow(tx, "e", out value));
            table.DeleteExistingRow(tx, "b");
            table.UpdateExistingRow(tx, "c", "33");
            table.UpdateExistingRow(tx, "d", "44");
            tx.Commit();

            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 2);
            Assert.True(table.TryGetRow(tx, "a", out value)); Assert.Equal("1", value);
            Assert.False(table.TryGetRow(tx, "b", out value));
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("33", value);
            Assert.True(table.TryGetRow(tx, "d", out value)); Assert.Equal("44", value);
            Assert.False(table.TryGetRow(tx, "e", out value));
            tx.Commit();

            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 3);
            table.UpdateExistingRow(tx, "d", "444");
            tx.Commit();

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);

            tx = await cache.StartTransactionAsync(0);
            Debug.Assert(tx.TxId == 5);
            table.UpdateExistingRow(tx, "d", "4444");
            tx.Commit();

            Assert.Equal(4, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);

            Assert.Collection(checkpoint.GetDeltas("A", 0),
                delta => Assert.Equal(("b", default, Writeback.Deleted), delta),
                delta => Assert.Equal(("c", "33", Writeback.Updated), delta),
                delta => Assert.Equal(("d", "444", Writeback.Updated), delta));

            tx = await cache.StartTransactionAsync(0);
            Assert.True(table.TryGetRow(tx, "a", out value)); Assert.Equal("1", value);
            Assert.False(table.TryGetRow(tx, "b", out value));
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("33", value);
            Assert.True(table.TryGetRow(tx, "d", out value)); Assert.Equal("4444", value);
            tx.Commit();
        }
    }
}
