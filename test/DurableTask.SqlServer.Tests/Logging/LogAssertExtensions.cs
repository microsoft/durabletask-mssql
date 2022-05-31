// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Logging
{
    using System.Collections.Generic;
    using System.Linq;
    using DurableTask.SqlServer.Logging;
    using Xunit;

    static class LogAssertExtensions
    {
        public static void AllowAdditionalLogs(this IEnumerable<LogEntry> logs) =>
            logs.ToList(); // Resolve the enumerable

        public static void EndOfLog(this IEnumerable<LogEntry> logs)
        {
            int count = logs.Count();
            Assert.True(count == 0, $"Expected no additional logs but found {count}.");
        }

        public static IEnumerable<LogEntry> Contains(this IEnumerable<LogEntry> logs, params LogAssert[] asserts)
            => logs.Contains(optional: false, asserts: asserts);

        public static IEnumerable<LogEntry> ContainsIf(this IEnumerable<LogEntry> logs, bool predicate, params LogAssert[] asserts) =>
            predicate ? logs.Contains(asserts) : logs;

        public static IEnumerable<LogEntry> Expect(this IEnumerable<LogEntry> logs, params LogAssert[] asserts)
        {
            int i = 0;
            foreach (LogEntry actual in logs)
            {
                if (actual.EventId == EventIds.GenericInfo)
                {
                    // Ignore generic info events, which can be non-deterministic
                    continue;
                }

                if (asserts.Length > i)
                {
                    LogAssert expected = asserts[i++];

                    // GenericInfo logs are not supported for validation
                    Assert.NotEqual(EventIds.GenericInfo, expected.EventId);

                    Assert.Equal(expected.EventName, actual.EventId.Name);
                    Assert.Equal(expected.EventId, actual.EventId.Id);
                    Assert.Equal(expected.Level, actual.LogLevel);
                    Assert.Contains(expected.MessageSubstring, actual.Message);

                    LogAssert.ValidateStructuredLogFields(actual);
                }
                else
                {
                    yield return actual;
                }
            }
        }

        public static IEnumerable<LogEntry> ExpectIf(this IEnumerable<LogEntry> logs, bool predicate, params LogAssert[] asserts) =>
            predicate ? logs.Expect(asserts) : logs;

        public static IEnumerable<LogEntry> OptionallyContainsIf(this IEnumerable<LogEntry> logs, bool predicate, params LogAssert[] asserts) =>
            predicate ? logs.Contains(optional: true, asserts: asserts) : logs;

        static IEnumerable<LogEntry> Contains(this IEnumerable<LogEntry> logs, bool optional, params LogAssert[] asserts)
        {
            var remaining = new HashSet<LogAssert>(asserts);

            foreach (LogEntry logEntry in logs)
            {
                LogAssert match = remaining.FirstOrDefault(assert =>
                    string.Equals(assert.EventName, logEntry.EventId.Name) &&
                    assert.EventId == logEntry.EventId.Id &&
                    assert.Level == logEntry.LogLevel &&
                    logEntry.Message.Contains(assert.MessageSubstring));

                if (match != null)
                {
                    remaining.Remove(match);
                }
                else
                {
                    yield return logEntry;
                }
            }

            if (!optional)
            {
                Assert.Empty(remaining);
            }
        }
    }
}
