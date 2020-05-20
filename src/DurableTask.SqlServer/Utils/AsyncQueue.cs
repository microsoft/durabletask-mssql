namespace DurableTask.SqlServer.Utils
{
    using System.Threading;
    using System.Threading.Channels;
    using System.Threading.Tasks;

    sealed class AsyncQueue<T>
    {
        readonly Channel<T> channel = Channel.CreateUnbounded<T>();

        public void Enqueue(T item)
        {
            this.channel.Writer.TryWrite(item);
        }

        public ValueTask<T> DequeueAsync(CancellationToken cancellationToken)
        {
            return this.channel.Reader.ReadAsync(cancellationToken);
        }

        public bool TryDequeue(out T item)
        {
            return this.channel.Reader.TryRead(out item);
        }
    }
}
