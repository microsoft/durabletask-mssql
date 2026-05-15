// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
#nullable enable
namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Data;
    using System.Data.SqlTypes;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using Xunit;

    public class SqlUtilsTraceContextTests
    {
        // The legacy reader on `main` uses this exact split. Tests that claim to exercise
        // backwards-compat must mirror it byte-for-byte, otherwise they prove nothing.
        static readonly char[] LegacyTraceContextSeparators = new char[] { '\n' };

        static (string? traceParent, string? traceState) SimulateLegacyReader(string payload)
        {
            string[] parts = payload.Split(LegacyTraceContextSeparators, count: 2, StringSplitOptions.RemoveEmptyEntries);
            string? traceParent = parts.Length > 0 ? parts[0] : null;
            string? traceState = parts.Length > 1 ? parts[1] : null;
            return (traceParent, traceState);
        }

        [Fact]
        public void GetTraceContext_RoundTripsExtendedTraceContextFields()
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 22, 30, 00, DateTimeKind.Utc);
            var traceContext = new DistributedTraceContext(
                traceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                traceState: "vendor=value")
            {
                Id = "00-0123456789abcdef0123456789abcdef-fedcba9876543210-01",
                SpanId = "fedcba9876543210",
            };

            var startedEvent = new ExecutionStartedEvent(-1, input: null)
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance",
                    ExecutionId = "execution",
                },
                ParentTraceContext = traceContext,
                Timestamp = timestamp,
            };

            SqlString serialized = SqlUtils.GetTraceContext(startedEvent);
            Assert.False(serialized.IsNull);
            // Wire format:
            //   line 1: traceparent
            //   line 2: tracestate, with our "durabletask-mssql=id:...;span:..." vendor key
            //           prepended (W3C tracestate is comma-separated key=value entries).
            // The legacy reader will see line 2 as the tracestate string verbatim and propagate
            // it through OpenTelemetry; W3C-compliant systems preserve unknown vendor keys.
            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\n" +
                "durabletask-mssql=id:00-0123456789abcdef0123456789abcdef-fedcba9876543210-01;span:fedcba9876543210," +
                "vendor=value",
                serialized.Value);

            using var reader = CreateExecutionStartedReader(serialized.Value, timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<ExecutionStartedEvent>(reader.GetHistoryEvent());
            Assert.NotNull(roundTrippedEvent.ParentTraceContext);
            Assert.Equal(traceContext.TraceParent, roundTrippedEvent.ParentTraceContext.TraceParent);
            Assert.Equal(traceContext.TraceState, roundTrippedEvent.ParentTraceContext.TraceState);
            Assert.Equal(traceContext.Id, roundTrippedEvent.ParentTraceContext.Id);
            Assert.Equal(traceContext.SpanId, roundTrippedEvent.ParentTraceContext.SpanId);
            Assert.Equal(new DateTimeOffset(timestamp), roundTrippedEvent.ParentTraceContext.ActivityStartTime);
        }

        // ROLLING-UPGRADE SAFETY — case 1: tracestate + Id/SpanId
        // Simulates what the pre-PR reader on `main` (Split('\n', 2, RemoveEmptyEntries)) sees
        // when a new worker writes the trace context for an event whose DistributedTraceContext
        // has both Id/SpanId and a user tracestate.
        //
        // The legacy reader must produce:
        //   - A clean W3C traceparent for parts[0] (no extra characters).
        //   - A single-line W3C-valid tracestate for parts[1] (no embedded newlines), so the
        //     value can flow into Activity.TraceStateString and propagate downstream unchanged.
        //
        // Our durabletask-mssql vendor key is allowed to appear inside parts[1] — that is the
        // whole point of using a W3C vendor key: legacy workers preserve unknown vendor keys
        // and propagate them downstream where newer workers can decode them.
        [Fact]
        public void GetTraceContext_LegacyReaderSeesCleanTraceParentAndTraceState_TraceStatePlusIds()
        {
            var traceContext = new DistributedTraceContext(
                traceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                traceState: "vendor=value")
            {
                Id = "00-0123456789abcdef0123456789abcdef-fedcba9876543210-01",
                SpanId = "fedcba9876543210",
            };

            var startedEvent = new ExecutionStartedEvent(-1, input: null)
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance",
                    ExecutionId = "execution",
                },
                ParentTraceContext = traceContext,
            };

            string payload = SqlUtils.GetTraceContext(startedEvent).Value;
            (string? legacyTraceParent, string? legacyTraceState) = SimulateLegacyReader(payload);

            // Legacy reader receives a clean traceparent...
            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                legacyTraceParent);

            // ...and a single-line tracestate value with no embedded newlines.
            Assert.NotNull(legacyTraceState);
            Assert.DoesNotContain('\n', legacyTraceState!);

            // The tracestate carries our vendor key (legacy workers preserve it) and the
            // user-supplied vendor entry.
            Assert.Contains("durabletask-mssql=id:", legacyTraceState!);
            Assert.Contains("vendor=value", legacyTraceState!);
        }

        // ROLLING-UPGRADE SAFETY — case 2: only Id/SpanId, no user tracestate.
        // Without a user tracestate, line 2 must still be a single line containing only the
        // vendor key. An older reader assigns that whole line to TraceState and propagates it.
        // CRITICALLY: line 2 must not contain embedded newlines or be missing entirely —
        // either failure mode would have leaked '@id=' garbage into TraceState in the prior
        // attempt at this PR.
        [Fact]
        public void GetTraceContext_LegacyReaderSeesCleanTraceState_IdsOnly()
        {
            var traceContext = new DistributedTraceContext(
                traceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
            {
                Id = "00-0123456789abcdef0123456789abcdef-fedcba9876543210-01",
                SpanId = "fedcba9876543210",
            };

            var startedEvent = new ExecutionStartedEvent(-1, input: null)
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance",
                    ExecutionId = "execution",
                },
                ParentTraceContext = traceContext,
            };

            string payload = SqlUtils.GetTraceContext(startedEvent).Value;
            (string? legacyTraceParent, string? legacyTraceState) = SimulateLegacyReader(payload);

            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                legacyTraceParent);

            Assert.NotNull(legacyTraceState);
            Assert.DoesNotContain('\n', legacyTraceState!);
            Assert.Equal(
                "durabletask-mssql=id:00-0123456789abcdef0123456789abcdef-fedcba9876543210-01;span:fedcba9876543210",
                legacyTraceState);
        }

        // ROLLING-UPGRADE SAFETY — case 3: legacy payload format (no Id/SpanId).
        // A worker on the new code writing a trace context that has no Id/SpanId emits the
        // exact same bytes as the pre-PR code. There is no compatibility cliff for these rows.
        [Fact]
        public void GetTraceContext_TraceStateOnlyPayload_MatchesLegacyByteForByte()
        {
            var traceContext = new DistributedTraceContext(
                traceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                traceState: "vendor=value");

            var startedEvent = new ExecutionStartedEvent(-1, input: null)
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance",
                    ExecutionId = "execution",
                },
                ParentTraceContext = traceContext,
            };

            string payload = SqlUtils.GetTraceContext(startedEvent).Value;

            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\nvendor=value",
                payload);
        }

        [Fact]
        public void GetHistoryEvent_PreservesLegacyTraceContextFormat()
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 22, 45, 00, DateTimeKind.Utc);
            const string traceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
            const string traceState = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE";

            using var reader = CreateExecutionStartedReader($"{traceParent}\n{traceState}", timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<ExecutionStartedEvent>(reader.GetHistoryEvent());
            Assert.NotNull(roundTrippedEvent.ParentTraceContext);
            Assert.Equal(traceParent, roundTrippedEvent.ParentTraceContext.TraceParent);
            Assert.Equal(traceState, roundTrippedEvent.ParentTraceContext.TraceState);
            Assert.Null(roundTrippedEvent.ParentTraceContext.Id);
            Assert.Null(roundTrippedEvent.ParentTraceContext.SpanId);
            Assert.Equal(new DateTimeOffset(timestamp), roundTrippedEvent.ParentTraceContext.ActivityStartTime);
        }

        [Fact]
        public void GetHistoryEvent_RoundTripsSubOrchestrationClientSpanId()
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 23, 00, 00, DateTimeKind.Utc);
            const string clientSpanId = "fedcba9876543210";

            var createdEvent = new SubOrchestrationInstanceCreatedEvent(eventId: 7)
            {
                ClientSpanId = clientSpanId,
                Input = "{}",
                Name = "MySubOrchestration",
                Timestamp = timestamp,
            };

            SqlString serialized = SqlUtils.GetTraceContext(createdEvent);
            Assert.False(serialized.IsNull);
            // Sub-orchestration rows had no TraceContext column populated on `main`, so the
            // legacy reader never inspects this column for SubOrchestrationInstanceCreated
            // events. The wire format is therefore optimized for the new reader and uses the
            // same vendor-key encoding so that all readers share a single parser.
            Assert.Equal(
                "durabletask-mssql=client:fedcba9876543210",
                serialized.Value);

            using var reader = CreateSubOrchestrationCreatedReader(serialized.Value, timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<SubOrchestrationInstanceCreatedEvent>(reader.GetHistoryEvent());
            Assert.Equal(clientSpanId, roundTrippedEvent.ClientSpanId);
        }

        // New-reader-on-new-row: when both a user tracestate and our vendor key are present,
        // the parser must split them cleanly so user tracestate flows through unchanged and
        // Id/SpanId are recovered from the vendor key.
        [Fact]
        public void GetHistoryEvent_NewReader_SeparatesVendorKeyFromUserTraceState()
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 23, 30, 00, DateTimeKind.Utc);
            string payload =
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\n" +
                "durabletask-mssql=id:00-0123456789abcdef0123456789abcdef-fedcba9876543210-01;span:fedcba9876543210," +
                "vendor=value,other=thing";

            using var reader = CreateExecutionStartedReader(payload, timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<ExecutionStartedEvent>(reader.GetHistoryEvent());
            Assert.NotNull(roundTrippedEvent.ParentTraceContext);

            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                roundTrippedEvent.ParentTraceContext.TraceParent);
            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-fedcba9876543210-01",
                roundTrippedEvent.ParentTraceContext.Id);
            Assert.Equal(
                "fedcba9876543210",
                roundTrippedEvent.ParentTraceContext.SpanId);

            // The vendor key has been stripped, but every user-supplied tracestate entry is
            // preserved in original order.
            Assert.Equal("vendor=value,other=thing", roundTrippedEvent.ParentTraceContext.TraceState);
        }

        // The user's tracestate may already contain entries that the legacy code propagated
        // through unmodified. A row where someone (any worker, old or new) has prepended our
        // vendor key must round-trip cleanly even if user entries appear before or after.
        [Theory]
        [InlineData(
            "durabletask-mssql=id:abc;span:def",
            "",
            "abc",
            "def")]
        [InlineData(
            "durabletask-mssql=id:abc;span:def,user=val",
            "user=val",
            "abc",
            "def")]
        [InlineData(
            "user=val,durabletask-mssql=id:abc;span:def",
            "user=val",
            "abc",
            "def")]
        [InlineData(
            "user=val, durabletask-mssql=id:abc;span:def ,other=thing",
            "user=val,other=thing",
            "abc",
            "def")]
        public void GetHistoryEvent_NewReader_ExtractsVendorKeyRegardlessOfPosition(
            string tracestate,
            string expectedUserTraceState,
            string expectedId,
            string expectedSpanId)
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 23, 45, 00, DateTimeKind.Utc);
            string payload = $"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\n{tracestate}";

            using var reader = CreateExecutionStartedReader(payload, timestamp);
            Assert.True(reader.Read());

            var ev = Assert.IsType<ExecutionStartedEvent>(reader.GetHistoryEvent());
            Assert.Equal(expectedId, ev.ParentTraceContext.Id);
            Assert.Equal(expectedSpanId, ev.ParentTraceContext.SpanId);
            Assert.Equal(
                string.IsNullOrEmpty(expectedUserTraceState) ? null : expectedUserTraceState,
                ev.ParentTraceContext.TraceState);
        }

        static DataTableReader CreateExecutionStartedReader(string traceContext, DateTime timestamp)
        {
            var table = new DataTable();
            table.Columns.Add("EventType", typeof(string));
            table.Columns.Add("TaskID", typeof(int));
            table.Columns.Add("PayloadText", typeof(string));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("InstanceID", typeof(string));
            table.Columns.Add("ExecutionID", typeof(string));
            table.Columns.Add("Tags", typeof(string));
            table.Columns.Add("Version", typeof(string));
            table.Columns.Add("TraceContext", typeof(string));
            table.Columns.Add("ParentInstanceID", typeof(string));
            table.Columns.Add("Timestamp", typeof(DateTime));
            table.Columns.Add("RuntimeStatus", typeof(string));

            DataRow row = table.NewRow();
            row["EventType"] = EventType.ExecutionStarted.ToString();
            row["TaskID"] = -1;
            row["PayloadText"] = DBNull.Value;
            row["Name"] = "TestOrchestration";
            row["InstanceID"] = "instance";
            row["ExecutionID"] = "execution";
            row["Tags"] = DBNull.Value;
            row["Version"] = DBNull.Value;
            row["TraceContext"] = traceContext;
            row["ParentInstanceID"] = DBNull.Value;
            row["Timestamp"] = timestamp;
            row["RuntimeStatus"] = OrchestrationStatus.Running.ToString();
            table.Rows.Add(row);

            return table.CreateDataReader();
        }

        static DataTableReader CreateSubOrchestrationCreatedReader(string traceContext, DateTime timestamp)
        {
            var table = new DataTable();
            table.Columns.Add("EventType", typeof(string));
            table.Columns.Add("TaskID", typeof(int));
            table.Columns.Add("PayloadText", typeof(string));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("TraceContext", typeof(string));
            table.Columns.Add("Timestamp", typeof(DateTime));

            DataRow row = table.NewRow();
            row["EventType"] = EventType.SubOrchestrationInstanceCreated.ToString();
            row["TaskID"] = 7;
            row["PayloadText"] = "{}";
            row["Name"] = "MySubOrchestration";
            row["TraceContext"] = traceContext;
            row["Timestamp"] = timestamp;
            table.Rows.Add(row);

            return table.CreateDataReader();
        }
    }
}
