// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class StoreOnlyTests
    {
        [Fact]
        public async Task Create()
        {
            var cache = new PipelinePersistentCache();
            var table = new TestTable("A", (key) =>
            {
                Assert.Fail("no load expected for this test");
                return default;
            });

            TxContext tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            table.CreateNonExistingRow(tx, "a", "1");
            table.CreateNonExistingRow(tx, "b", "2");
            table.CreateNonExistingRow(tx, "c", "3");
            tx.Commit();

            tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            Assert.True(table.TryGetRow(tx, "a", out var value)); Assert.Equal("1", value);
            Assert.True(table.TryGetRow(tx, "b", out value)); Assert.Equal("2", value);
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("3", value);
            tx.Commit();

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);

            Assert.Equal(3, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);

            Assert.Collection(checkpoint.GetDeltas("A", 0),
                delta => Assert.Equal(("a", "1", Writeback.Created), delta),
                delta => Assert.Equal(("b", "2", Writeback.Created), delta),
                delta => Assert.Equal(("c", "3", Writeback.Created), delta));

            tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            Assert.True(table.TryGetRow(tx, "a", out value)); Assert.Equal("1", value);
            Assert.True(table.TryGetRow(tx, "b", out value)); Assert.Equal("2", value);
            Assert.True(table.TryGetRow(tx, "c", out value)); Assert.Equal("3", value);
            tx.Commit();
        }

        
    }
}
