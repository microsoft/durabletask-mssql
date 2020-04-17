namespace DurableTask.SqlServer.Utils
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class AsyncQueue<T> : IDisposable
    {
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(0);
        readonly ConcurrentQueue<T> innerQueue = new ConcurrentQueue<T>();

        public int Count => this.innerQueue.Count;

        public void Enqueue(T item)
        {
            this.innerQueue.Enqueue(item);
            this.semaphore.Release();
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                await this.semaphore.WaitAsync(cancellationToken);

                if (this.innerQueue.TryDequeue(out T item))
                {
                    return item;
                }
            }
        }

        public bool TryDequeue(out T item)
        {
            if (this.semaphore.Wait(0))
            {
                return this.innerQueue.TryDequeue(out item);
            }

            item = default!;
            return false;
        }

        public void Dispose()
        {
            this.semaphore.Dispose();
        }
    }
}
