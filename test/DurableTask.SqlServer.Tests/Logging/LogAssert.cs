// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

        public static LogAssert AcquiredAppLock() =>
            new LogAssert(300, "AcquiredAppLock", LogLevel.Information, "");

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

        public static LogAssert CommandCompleted(string commandSnippet) =>
            new LogAssert(311, "CommandCompleted", LogLevel.Debug, commandSnippet);

        public static LogAssert CheckedDatabase()
            => CommandCompleted("SELECT 1 FROM sys.databases WHERE name = @databaseName");

        public static LogAssert CreatedDatabase(string name) =>
            new LogAssert(312, "CreatedDatabase", LogLevel.Information, name);

        public static void LogEntryCount(TestLogProvider logProvider, int expected) =>
            Assert.Equal(expected, GetLogs(logProvider).Count);

        static IReadOnlyCollection<LogEntry> GetLogs(TestLogProvider logProvider)
        {
            Assert.True(logProvider.TryGetLogs("DurableTask.SqlServer", out IReadOnlyCollection<LogEntry> logs));
            return logs;
        }

        public static IReadOnlyCollection<LogEntry> For(TestLogProvider logProvider)
        {
            Assert.True(logProvider.TryGetLogs("DurableTask.SqlServer", out IReadOnlyCollection<LogEntry> logs));
            return logs;
        }

        public static void Sequence(TestLogProvider logProvider, params LogAssert[] asserts) =>
            For(logProvider).Expect(asserts).EndOfLog();

        public static void Contains(TestLogProvider logProvider, params LogAssert[] asserts) =>
            For(logProvider).Contains(asserts).AllowAdditionalLogs();

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
                case EventIds.ProcessingFailure:
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("ExecutionId"));
                    Assert.True(fields.ContainsKey("Details"));
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
                case EventIds.ReplicaCountChangeRecommended:
                    Assert.Contains("CurrentCount", fields);
                    Assert.Contains("RecommendedCount", fields);
                    break;
                case EventIds.CommandCompleted:
                    Assert.True(fields.ContainsKey("CommandText"));
                    Assert.True(fields.ContainsKey("LatencyMs"));
                    break;
                case EventIds.CreatedDatabase:
                    Assert.True(fields.ContainsKey("DatabaseName"));
                    break;
                case EventIds.DiscardingEvent:
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("EventType"));
                    Assert.True(fields.ContainsKey("TaskEventId"));
                    Assert.True(fields.ContainsKey("Details"));
                    break;
                case EventIds.GenericInfo:
                    Assert.True(fields.ContainsKey("InstanceId"));
                    Assert.True(fields.ContainsKey("Details"));
                    break;
                default:
                    throw new ArgumentException($"Log event {log.EventId} is not known. Does it need to be added to the log validator?", nameof(log));
            }
        }

        public static T FieldEquals<T>(LogEntry logEntry, string fieldName, T expectedValue)
        {
            T convertedValue = GetFieldValue<T>(logEntry, fieldName);
            Assert.Equal(expectedValue, convertedValue);
            return convertedValue;
        }

        public static T GetFieldValue<T>(LogEntry logEntry, string fieldName)
        {
            var structuredEvent = logEntry.State as IReadOnlyDictionary<string, object>;
            Assert.NotNull(structuredEvent);

            IReadOnlyDictionary<string, object> eventData = 
                Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(logEntry.State);

            object fieldValue = Assert.Contains(fieldName, eventData);
            T convertedValue = Assert.IsType<T>(fieldValue);
            return convertedValue;
        }
    }
}
