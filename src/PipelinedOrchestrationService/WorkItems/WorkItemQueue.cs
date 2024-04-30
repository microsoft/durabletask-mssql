﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    class WorkItemQueue<T> : IDisposable
    {
        readonly ConcurrentQueue<T> work = new ConcurrentQueue<T>();
        readonly SemaphoreSlim count = new SemaphoreSlim(0);

        public int Load => this.count.CurrentCount;

        public void Add(T element)
        {
            this.work.Enqueue(element);
            this.count.Release();
        }

        public void Dispose()
        {
            this.count.Dispose();
        }

        public async Task<T?> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            try
            {
                T? result = default;

                bool success = await this.count.WaitAsync((int)timeout.TotalMilliseconds, cancellationToken);

                if (success)
                {
                    success = this.work.TryDequeue(out result);

                    // we should always succeed here; but just for the unlikely case that we don't 
                    // (e.g. if concurrent queue implementation is not linearizable),
                    // put the count back up by one if we didn't actually get an element
                    if (!success)
                    {
                        this.count.Release();
                    }
                }

                return result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return default;
            }
        }
    }
}
