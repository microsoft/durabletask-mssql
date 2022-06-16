// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Utils
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    class PartitionedQueue<TKey, TValue>
    {
        readonly ConcurrentDictionary<TKey, List<TValue>> partitionsByKey = new ConcurrentDictionary<TKey, List<TValue>>();
        readonly AsyncQueue<List<TValue>> partitionsByOrder = new AsyncQueue<List<TValue>>();

        readonly Func<TValue, TKey> partitionIdentifier;

        public PartitionedQueue(Func<TValue, TKey> partitionIdentifier)
        {
            this.partitionIdentifier = partitionIdentifier;
        }

        public int PartitionCount => this.partitionsByKey.Count;

        public void Add(TValue item)
        {
            TKey partitionId = this.partitionIdentifier(item);

            bool added = false;
            List<TValue> partition;

            // AddOrUpdate is not atomic, thus we use a lock here
            // to avoid creating multiple List<T> objects.
            lock (this.partitionsByKey)
            {
                partition = this.partitionsByKey.AddOrUpdate(
                    partitionId,
                    addValueFactory: id =>
                    {
                        added = true;
                        return new List<TValue> { item };
                    },
                    updateValueFactory: (id, group) =>
                    {
                        group.Add(item);
                        return group;
                    });
            }

            // The group must be added to the queue AFTER it is added to the dictionary
            // to help ensure thread safety.
            if (added)
            {
                this.partitionsByOrder.Enqueue(partition);
            }
        }

        public async Task<IReadOnlyList<TValue>> TakeNextPartitionAsync(CancellationToken cancellation)
        {
            // TODO: Need to ensure this is thread-safe
            List<TValue> group = await this.partitionsByOrder.DequeueAsync(cancellation);
            TKey groupId = this.partitionIdentifier(group[0]);
            this.partitionsByKey.TryRemove(groupId, out _);
            return group;
        }
    }
}
