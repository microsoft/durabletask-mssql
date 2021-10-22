﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Logging;
    using Xunit.Abstractions;

    public sealed class TestLogProvider : ILoggerProvider
    {
        readonly ITestOutputHelper output;
        readonly ConcurrentDictionary<string, TestLogger> loggers;

        public TestLogProvider(ITestOutputHelper output)
        {
            this.output = output ?? throw new ArgumentNullException(nameof(output));
            this.loggers = new ConcurrentDictionary<string, TestLogger>(StringComparer.OrdinalIgnoreCase);
        }

        public bool TryGetLogs(string category, out IReadOnlyCollection<LogEntry> logs)
        {
            if (this.loggers.TryGetValue(category, out TestLogger logger))
            {
                logs = logger.GetLogs();
                return true;
            }

            logs = Array.Empty<LogEntry>();
            return false;
        }

        public void Clear()
        {
            foreach (TestLogger logger in this.loggers.Values.OfType<TestLogger>())
            {
                logger.ClearLogs();
            }
        }

        ILogger ILoggerProvider.CreateLogger(string categoryName)
        {
            return this.loggers.GetOrAdd(categoryName, _ => new TestLogger(categoryName, this.output));
        }

        void IDisposable.Dispose()
        {
            // no-op
        }

        class TestLogger : ILogger
        {
            readonly string category;
            readonly ITestOutputHelper output;
            readonly ConcurrentQueue<LogEntry> entries;

            public TestLogger(string category, ITestOutputHelper output)
            {
                this.category = category;
                this.output = output;
                this.entries = new ConcurrentQueue<LogEntry>();
            }

            public IReadOnlyCollection<LogEntry> GetLogs() => this.entries;

            public void ClearLogs() => this.entries.Clear();

            IDisposable ILogger.BeginScope<TState>(TState state) => null;

            bool ILogger.IsEnabled(LogLevel logLevel) => true;

            void ILogger.Log<TState>(
                LogLevel level,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                var entry = new LogEntry(
                    this.category,
                    level,
                    eventId,
                    exception,
                    formatter(state, exception),
                    state);
                this.entries.Enqueue(entry);

                try
                {
                    this.output.WriteLine(entry.ToString());
                }
                catch (InvalidOperationException)
                {
                    // Expected when tests are shutting down
                }
            }
        }
    }
}
