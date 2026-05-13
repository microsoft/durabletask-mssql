// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
            // Wire format is rolling-upgrade safe: line 2 is RAW tracestate (no @tracestate= prefix)
            // so older workers reading new rows continue to extract tracestate correctly. Newer
            // @-prefixed fields go on lines 3+ and are ignored by older readers.
            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\nvendor=value\n@id=00-0123456789abcdef0123456789abcdef-fedcba9876543210-01\n@spanid=fedcba9876543210",
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

        // Rolling-upgrade safety: a worker running the old code (which only knows about
        // line 1 = traceparent and line 2 = raw tracestate) must still extract the correct
        // traceparent and tracestate when it reads a row written by a worker running the
        // new code. The new code adds @id= / @spanid= on lines 3+, but those lines must NOT
        // contaminate the tracestate slot on line 2. This test asserts the wire-format
        // contract that protects that invariant.
        [Fact]
        public void GetTraceContext_NewWriterPlacesRawTraceStateOnLineTwo_ForOldReaderCompat()
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
            string[] parts = payload.Split('\n');

            // Simulate the legacy reader: parts[0] = traceparent, parts[1] = tracestate (raw).
            Assert.Equal("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", parts[0]);
            Assert.Equal("vendor=value", parts[1]);
            // The legacy reader stops at parts[1]; everything beyond is ignored.
            Assert.DoesNotContain(parts[1], "@");
        }

        // Rolling-upgrade safety for the empty-tracestate case: when the new writer has no
        // tracestate but does have Id/SpanId, line 2 must still exist as an empty placeholder
        // so the @-prefixed lines that follow never land in the tracestate slot.
        [Fact]
        public void GetTraceContext_NewWriterEmitsEmptyTraceStateLine_WhenOnlyIdAndSpanIdSet()
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
            string[] parts = payload.Split('\n');

            Assert.Equal("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", parts[0]);
            // Line 2 is an empty placeholder. An older reader will set TraceState = string.Empty,
            // which is functionally indistinguishable from "no tracestate". Without this empty
            // placeholder, the older reader would see "@id=..." on line 2 and store that bogus
            // value as the tracestate.
            Assert.Equal(string.Empty, parts[1]);
            Assert.StartsWith("@id=", parts[2]);
            Assert.StartsWith("@spanid=", parts[3]);
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
            // Wire format is rolling-upgrade safe:
            //   line 1: empty (no traceparent for a sub-orch-only payload)
            //   line 2: empty placeholder (reserved tracestate slot — older readers see "",
            //           not "@clientspanid=", so they don't misinterpret it as tracestate)
            //   line 3: @clientspanid=...
            Assert.Equal("\n\n@clientspanid=fedcba9876543210", serialized.Value);

            using var reader = CreateSubOrchestrationCreatedReader(serialized.Value, timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<SubOrchestrationInstanceCreatedEvent>(reader.GetHistoryEvent());
            Assert.Equal(clientSpanId, roundTrippedEvent.ClientSpanId);
        }

        // Older histories may still contain the legacy single-line "@clientspanid=..." payload
        // that was written before line 1 was reserved for traceparent. This regression makes sure
        // the reader keeps parsing such rows correctly, so an upgrade does not break running
        // orchestrations whose history was persisted by an older build.
        [Fact]
        public void GetHistoryEvent_ParsesLegacyClientSpanIdPayloadWithoutLeadingNewline()
        {
            DateTime timestamp = new DateTime(2026, 04, 15, 23, 00, 00, DateTimeKind.Utc);
            const string clientSpanId = "abcdef0123456789";

            // Legacy on-the-wire format: the @clientspanid= prefix sits on line 1.
            string legacyPayload = "@clientspanid=" + clientSpanId;

            using var reader = CreateSubOrchestrationCreatedReader(legacyPayload, timestamp);
            Assert.True(reader.Read());

            var roundTrippedEvent = Assert.IsType<SubOrchestrationInstanceCreatedEvent>(reader.GetHistoryEvent());
            Assert.Equal(clientSpanId, roundTrippedEvent.ClientSpanId);
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
