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

            TxContext tx = await cache.StartTransactionAsync(CancellationToken.None);

            table.Create(tx, "a", "1");
            table.Create(tx, "b", "2");
            table.Create(tx, "c", "3");

            tx.Commit();
              

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);

            Assert.Equal(2, checkpoint.Seqno);
            Assert.Empty(checkpoint.Actions);

            Assert.Collection(checkpoint.GetDeltas("A"),
                delta => Assert.Equal(("a", "1", Writeback.Created), delta),
                delta => Assert.Equal(("b", "2", Writeback.Created), delta),
                delta => Assert.Equal(("c", "3", Writeback.Created), delta));   
        }

        [Fact]
        public async Task CreateDelete()
        {
            var cache = new PipelinePersistentCache();
            var table = new TestTable("A", (key) =>
            {
                Assert.Fail("no load expected for this test");
                return default;
            });

            TxContext tx = await cache.StartTransactionAsync(CancellationToken.None);

            table.Create(tx, "a", "1");
            table.Create(tx, "b", "2");
            table.Create(tx, "c", "3");

            table.Delete(tx, "b");

            tx.Commit();

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);

            Assert.Equal(2, checkpoint.Seqno);
            Assert.Empty(checkpoint.Actions);

            Assert.Collection(checkpoint.GetDeltas("A"),
                delta => Assert.Equal(("a", "1", Writeback.Created), delta),
                delta => Assert.Equal(("c", "3", Writeback.Created), delta));
        }
    }
}
