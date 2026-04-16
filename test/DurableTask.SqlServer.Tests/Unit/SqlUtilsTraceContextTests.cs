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
            Assert.Equal(
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01\n@tracestate=vendor=value\n@id=00-0123456789abcdef0123456789abcdef-fedcba9876543210-01\n@spanid=fedcba9876543210",
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
            Assert.Equal("@clientspanid=fedcba9876543210", serialized.Value);

            using var reader = CreateSubOrchestrationCreatedReader(serialized.Value, timestamp);
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
