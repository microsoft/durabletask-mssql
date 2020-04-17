namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Logging;
    using Xunit.Abstractions;

    sealed class TestLogProvider : ILoggerProvider
    {
        readonly ITestOutputHelper output;
        readonly ConcurrentDictionary<string, TestLogger> loggers;

        public TestLogProvider(ITestOutputHelper output)
        {
            this.output = output ?? throw new ArgumentNullException(nameof(output));
            this.loggers = new ConcurrentDictionary<string, TestLogger>(StringComparer.OrdinalIgnoreCase);
        }

        public bool TryGetLogs(string category, out IEnumerable<LogEntry> logs)
        {
            if (this.loggers.TryGetValue(category, out TestLogger? logger))
            {
                logs = logger.GetLogs();
                return true;
            }

            logs = Enumerable.Empty<LogEntry>();
            return false;
        }

        ILogger ILoggerProvider.CreateLogger(string categoryName)
        {
            return this.loggers.GetOrAdd(categoryName, _ => new TestLogger(this.output));
        }

        void IDisposable.Dispose()
        {
            // no-op
        }

        class TestLogger : ILogger
        {
            readonly ITestOutputHelper output;
            readonly List<LogEntry> entries;

            public TestLogger(ITestOutputHelper output)
            {
                this.output = output;
                this.entries = new List<LogEntry>();
            }

            public IReadOnlyCollection<LogEntry> GetLogs() => this.entries.AsReadOnly();

            IDisposable? ILogger.BeginScope<TState>(TState state) => null;

            bool ILogger.IsEnabled(LogLevel logLevel) => true;

            void ILogger.Log<TState>(
                LogLevel level,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                var entry = new LogEntry(level, formatter(state, exception));
                this.entries.Add(entry);
                this.output.WriteLine(entry.ToString());
            }
        }
    }
}
