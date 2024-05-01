// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public class BasicApiTests
    {
        [Fact]
        public async Task EmptyCheckpoints()
        {
            var cache = new PipelinePersistentCache();
            var checkpoint = new TestCheckpoint();

            await cache.CollectNextCheckpointAsync(checkpoint);
            Assert.Equal(1, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);

            await cache.CollectNextCheckpointAsync(checkpoint);
            Assert.Equal(2, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);
        }

        [Fact]
        public async Task EmptyTransaction()
        {
            var cache = new PipelinePersistentCache();

            TxContext tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            Assert.Equal(1, tx.TxId);
            tx.Commit();

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);
            Assert.Equal(2, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Empty(checkpoint.Actions);
        }

        [Fact]
        public async Task Notifications()
        {
            var cache = new PipelinePersistentCache();

            TaskCompletionSource completed = new();
            TaskCompletionSource persisted = new();

            Action whenCompleted = () => completed.TrySetResult();
            Action whenPersisted = () => persisted.TrySetResult();

            TxContext tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            Assert.Equal(1, tx.TxId);
            tx.WhenCompleted(whenCompleted);
            tx.WhenPersisted(whenPersisted);

            Assert.False(completed.Task.IsCompleted);
            Assert.False(persisted.Task.IsCompleted);

            tx.Commit();

            await completed.Task;

            Assert.False(persisted.Task.IsCompleted);

            var checkpoint = new TestCheckpoint();
            await cache.CollectNextCheckpointAsync(checkpoint);
            Assert.Equal(2, checkpoint.PartitionMetaDatas[0].LastCheckpointId);
            Assert.Collection(checkpoint.Actions, (action) => whenPersisted.Equals(action));

            Assert.False(persisted.Task.IsCompleted);
        }

        [Fact]
        public async Task TxNotification()
        {
            var cache = new PipelinePersistentCache();

            TaskCompletionSource<long> completed = new();
            cache.AddTransactionCompletionListener(0, id => completed.TrySetResult(id));

            TxContext tx = await cache.StartTransactionAsync(0, CancellationToken.None);
            Assert.Equal(1, tx.TxId);

            Assert.False(completed.Task.IsCompleted);

            tx.Commit();
            var result = await completed.Task;

            Assert.Equal(1, result);
        }
    }
}