namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Logging;
    using Xunit;

    class LogAssert
    {
        LogAssert(int eventId, string eventName, LogLevel level, string messageSubstring)
        {
            this.EventId = eventId;
            this.EventName = eventName;
            this.Level = level;
            this.MessageSubstring = messageSubstring;
        }

        public int EventId { get; }

        public string EventName { get; }

        public LogLevel Level { get; }

        public string MessageSubstring { get; }

        public override string ToString()
        {
            return $"[{this.EventId:000}] {this.EventName} [{this.Level}] *{this.MessageSubstring}*";
        }

        public static void NoWarningsOrErrors(TestLogProvider logProvider)
            => Assert.Empty(GetLogs(logProvider).Where(entry => entry.LogLevel > LogLevel.Information));

        public static LogAssert AcquiredAppLock(int statusCode = 0) =>
            new LogAssert(300, "AcquiredAppLock", LogLevel.Information, $"Acquired app lock. Status code: {statusCode}");

        public static LogAssert ExecutedSqlScript(string scriptName) =>
            new LogAssert(301, "ExecutedSqlScript", LogLevel.Information, scriptName);

        public static LogAssert SprocCompleted(string sprocName) =>
            new LogAssert(302, "SprocCompleted", LogLevel.Trace, sprocName);

        public static void LogEntryCount(TestLogProvider logProvider, int expected) =>
            Assert.Equal(expected, GetLogs(logProvider).Count());

        static IEnumerable<LogEntry> GetLogs(TestLogProvider logProvider)
        {
            Assert.True(logProvider.TryGetLogs("DurableTask.SqlServer", out IEnumerable<LogEntry> logs));
            return logs;
        }

        public static void Sequence(TestLogProvider logProvider, params LogAssert[] asserts)
        {
            LogEntry[] actualLogs = GetLogs(logProvider).ToArray();

            string expected = Environment.NewLine + string.Join(Environment.NewLine, asserts.Select(log => log.EventName)) + Environment.NewLine;
            string actual = Environment.NewLine + string.Join(Environment.NewLine, actualLogs.Select(log => log.EventId.Name)) + Environment.NewLine;

            for (int i = 0; i < actualLogs.Length; i++)
            {
                Assert.True(asserts.Length > i, $"{asserts.Length} log entries were expected but {actualLogs.Length} were found. Expected:{expected}Actual:{actual}");

                Assert.Equal(asserts[i].EventName, actualLogs[i].EventId.Name);
                Assert.Equal(asserts[i].EventId, actualLogs[i].EventId.Id);
                Assert.Equal(asserts[i].Level, actualLogs[i].LogLevel);
                Assert.Contains(asserts[i].MessageSubstring, actualLogs[i].Message);
            }

            // If this fails, it means that more logs were expected.
            Assert.True(
                asserts.Length == actualLogs.Length,
                $"{asserts.Length} log entries were expected but only {actualLogs.Length} were found. Expected:{expected}Actual:{actual}");
        }
    }
}
