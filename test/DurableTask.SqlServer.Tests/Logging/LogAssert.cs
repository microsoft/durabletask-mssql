namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using DurableTask.SqlServer.Logging;
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

        // WARNING: Changing any of these hardcoded LogAssert constructor parameters is potentially breaking!
        public static LogAssert AcquiredAppLock(int statusCode = 0) =>
            new LogAssert(300, "AcquiredAppLock", LogLevel.Information, $"status code = {statusCode}");

        public static LogAssert ExecutedSqlScript(string scriptName) =>
            new LogAssert(301, "ExecutedSqlScript", LogLevel.Information, scriptName);

        public static LogAssert SprocCompleted(string sprocName) =>
            new LogAssert(302, "SprocCompleted", LogLevel.Debug, sprocName);

        public static LogAssert ProcessingError(string detailsSubstring) =>
            new LogAssert(303, "ProcessingError", LogLevel.Error, detailsSubstring);

        public static LogAssert SchedulingLocalActivityEvent(string name) =>
            new LogAssert(304, "SchedulingLocalActivity", LogLevel.Information, name);

        public static LogAssert CheckpointStarting(string name) =>
            new LogAssert(305, "CheckpointStarting", LogLevel.Information, name);

        public static LogAssert CheckpointCompleted(string name) =>
            new LogAssert(306, "CheckpointCompleted", LogLevel.Information, name);

        public static LogAssert DuplicateExecutionDetected(string name) =>
            new LogAssert(307, "DuplicateExecutionDetected", LogLevel.Warning, name);

        public static void LogEntryCount(TestLogProvider logProvider, int expected) =>
            Assert.Equal(expected, GetLogs(logProvider).Count());

        static IEnumerable<LogEntry> GetLogs(TestLogProvider logProvider)
        {
            Assert.True(logProvider.TryGetLogs("DurableTask.SqlServer", out IReadOnlyCollection<LogEntry> logs));
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

                ValidateStructuredLogFields(actualLogs[i]);
            }

            // If this fails, it means that more logs were expected.
            Assert.True(
                asserts.Length == actualLogs.Length,
                $"{asserts.Length} log entries were expected but only {actualLogs.Length} were found. Expected:{expected}Actual:{actual}");
        }

        public static void Contains(TestLogProvider logProvider, params LogAssert[] asserts)
        {
            var remaining = new HashSet<LogAssert>(asserts);

            foreach (LogEntry logEntry in GetLogs(logProvider))
            {
                foreach (LogAssert assert in remaining.ToArray())
                {
                    if (string.Equals(assert.EventName, logEntry.EventId.Name) &&
                        assert.EventId == logEntry.EventId.Id &&
                        assert.Level == logEntry.LogLevel &&
                        logEntry.Message.Contains(assert.MessageSubstring))
                    {
                        remaining.Remove(assert);
                    }
                }
            }

            Assert.Empty(remaining);
        }

        internal static void ValidateStructuredLogFields(LogEntry log)
        {
            // All log entries are expected to have dictionary state
            var fields = log.State as IReadOnlyDictionary<string, object>;
            Assert.NotNull(fields);
            Assert.NotEmpty(fields);

            switch (log.EventId.Id)
            {
                case EventIds.AcquiredAppLock:
                    Assert.True(fields.ContainsKey("StatusCode"));
                    Assert.True(fields.ContainsKey("LatencyMs"));
                    break;
                case EventIds.ExecutedSqlScript:
                    Assert.True(fields.ContainsKey("Name"));
                    Assert.True(fields.ContainsKey("LatencyMs"));
                    break;
                case EventIds.SprocCompleted:
                    Assert.True(fields.ContainsKey("Name"));
                    Assert.True(fields.ContainsKey("LatencyMs"));
                    break;
                case EventIds.CheckpointStarting:
                    Assert.True(fields.ContainsKey("Name"));
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("ExecutionId"));
                    Assert.True(fields.ContainsKey("Status"));
                    break;
                case EventIds.CheckpointCompleted:
                    Assert.True(fields.ContainsKey("Name"));
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("ExecutionId"));
                    Assert.True(fields.ContainsKey("LatencyMs"));
                    break;
                case EventIds.DuplicateExecutionDetected:
                    Assert.True(fields.ContainsKey("Name"));
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("ExecutionId"));
                    break;
                default:
                    throw new ArgumentException($"Log event {log.EventId} is not known. Does it need to be added to the log validator?", nameof(log));
            }
        }
    }
}
