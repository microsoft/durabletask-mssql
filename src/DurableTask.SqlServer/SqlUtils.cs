namespace DurableTask.SqlServer
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlTypes;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;
    using SemVersion;

    static class SqlUtils
    {
        public static string? GetStringOrNull(this DbDataReader reader, int columnIndex)
        {
            return reader.IsDBNull(columnIndex) ? null : reader.GetString(columnIndex);
        }

        public static TaskMessage GetTaskMessage(this DbDataReader reader)
        {
            return new TaskMessage
            {
                SequenceNumber = GetSequenceNumber(reader),
                Event = reader.GetHistoryEvent(),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetInstanceId(reader),
                    ExecutionId = GetExecutionId(reader),
                },
            };
        }

        public static HistoryEvent GetHistoryEvent(this DbDataReader reader, bool isOrchestrationHistory = false)
        {
            string eventTypeString = (string)reader["EventType"];
            if (!Enum.TryParse(eventTypeString, out EventType eventType))
            {
                throw new InvalidOperationException($"Unknown event type '{eventTypeString}'.");
            }

            int eventId = GetTaskId(reader);

            HistoryEvent historyEvent;
            switch(eventType)
            {
                case EventType.ContinueAsNew:
                    historyEvent = new ContinueAsNewEvent(eventId, GetPayloadText(reader));
                    break;
                case EventType.EventRaised:
                    historyEvent = new EventRaisedEvent(eventId, GetPayloadText(reader))
                    {
                        Name = GetName(reader),
                    };
                    break;
                case EventType.EventSent:
                    historyEvent = new EventSentEvent(eventId)
                    {
                        Input = GetPayloadText(reader),
                        Name = GetName(reader),
                        InstanceId = GetInstanceId(reader),
                    };
                    break;
                case EventType.ExecutionCompleted:
                    historyEvent = new ExecutionCompletedEvent(
                        eventId,
                        result: GetPayloadText(reader),
                        orchestrationStatus: OrchestrationStatus.Completed);
                    break;
                case EventType.ExecutionFailed:
                    historyEvent = new ExecutionCompletedEvent(
                        eventId,
                        result: GetPayloadText(reader),
                        orchestrationStatus: OrchestrationStatus.Failed);
                    break;
                case EventType.ExecutionStarted:
                    historyEvent = new ExecutionStartedEvent(eventId, GetPayloadText(reader))
                    {
                        Name = GetName(reader),
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = GetInstanceId(reader),
                            ExecutionId = GetExecutionId(reader),
                        },
                        ParentInstance = null, // TODO
                        Tags = null, // TODO
                        Version = null, // TODO
                    };
                    break;
                case EventType.ExecutionTerminated:
                    historyEvent = new ExecutionTerminatedEvent(eventId, GetPayloadText(reader));
                    break;
                case EventType.GenericEvent:
                    historyEvent = new GenericEvent(eventId, GetPayloadText(reader));
                    break;
                case EventType.OrchestratorCompleted:
                    historyEvent = new OrchestratorCompletedEvent(eventId);
                    break;
                case EventType.OrchestratorStarted:
                    historyEvent = new OrchestratorStartedEvent(eventId);
                    break;
                case EventType.SubOrchestrationInstanceCompleted:
                    historyEvent = new SubOrchestrationInstanceCompletedEvent(eventId, GetTaskId(reader), GetPayloadText(reader));
                    break;
                case EventType.SubOrchestrationInstanceCreated:
                    historyEvent = new SubOrchestrationInstanceCreatedEvent(eventId)
                    {
                        Input = GetPayloadText(reader),
                        InstanceId = null, // TODO
                        Name = GetName(reader),
                        Version = null,
                    };
                    break;
                case EventType.SubOrchestrationInstanceFailed:
                    historyEvent = new SubOrchestrationInstanceFailedEvent(
                        eventId,
                        taskScheduledId: GetTaskId(reader),
                        reason: GetReason(reader),
                        details: GetPayloadText(reader));
                    break;
                case EventType.TaskCompleted:
                    historyEvent = new TaskCompletedEvent(
                        eventId,
                        taskScheduledId: GetTaskId(reader),
                        result: GetPayloadText(reader));
                    break;
                case EventType.TaskFailed:
                    historyEvent = new TaskFailedEvent(
                        eventId,
                        taskScheduledId: GetTaskId(reader),
                        reason: GetReason(reader),
                        details: GetPayloadText(reader));
                    break;
                case EventType.TaskScheduled:
                    historyEvent = new TaskScheduledEvent(eventId)
                    {
                        Input = GetPayloadText(reader),
                        Name = GetName(reader),
                        Version = null, // TODO
                    };
                    break;
                case EventType.TimerCreated:
                    historyEvent = new TimerCreatedEvent(eventId)
                    {
                        FireAt = GetVisibleTime(reader),
                    };
                    break;
                case EventType.TimerFired:
                    historyEvent = new TimerFiredEvent(eventId)
                    {
                        FireAt = GetVisibleTime(reader),
                        TimerId = GetTaskId(reader),
                    };
                    break;
                default:
                    throw new InvalidOperationException($"Don't know how to interpret '{eventType}'.");
            }

            historyEvent.Timestamp = GetTimestamp(reader);
            historyEvent.IsPlayed = isOrchestrationHistory && (bool)reader["IsPlayed"];
            return historyEvent;
        }

        public static OrchestrationState GetOrchestrationState(this DbDataReader reader)
        {
            return new OrchestrationState
            {
                CompletedTime = reader.GetUtcDateTimeOrNull(reader.GetOrdinal("CompletedTime")) ?? default,
                CreatedTime = reader.GetUtcDateTimeOrNull(reader.GetOrdinal("CreatedTime")) ?? default,
                Input = reader.GetStringOrNull(reader.GetOrdinal("InputText")),
                LastUpdatedTime = reader.GetUtcDateTimeOrNull(reader.GetOrdinal("LastUpdatedTime")) ?? default,
                Name = GetName(reader),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetInstanceId(reader),
                    ExecutionId = GetExecutionId(reader),
                },
                OrchestrationStatus = (OrchestrationStatus)Enum.Parse(
                    typeof(OrchestrationStatus),
                    GetStringOrNull(reader, reader.GetOrdinal("RuntimeStatus"))),
                Output = GetStringOrNull(reader, reader.GetOrdinal("OutputText")),
                Status = GetStringOrNull(reader, reader.GetOrdinal("CustomStatusText")),
            };
        }

        internal static DateTime? GetVisibleTime(HistoryEvent historyEvent)
        {
            switch (historyEvent.EventType)
            {
                case EventType.TimerCreated:
                    return ((TimerCreatedEvent)historyEvent).FireAt;
                case EventType.TimerFired:
                    return ((TimerFiredEvent)historyEvent).FireAt;
                default:
                    return null;
            }
        }

        internal static SqlString GetRuntimeStatus(HistoryEvent historyEvent)
        {
            return DTUtils.GetRuntimeStatus(historyEvent)?.ToString() ?? SqlString.Null;
        }

        internal static SqlString GetName(HistoryEvent historyEvent)
        {
            return DTUtils.GetName(historyEvent) ?? SqlString.Null;
        }

        static string? GetName(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Name");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        internal static SqlInt32 GetTaskId(HistoryEvent historyEvent)
        {
            int taskEventId = DTUtils.GetTaskEventId(historyEvent);
            return taskEventId >= 0 ? new SqlInt32(taskEventId) : SqlInt32.Null;
        }

        public static int GetTaskId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("TaskID");
            return reader.IsDBNull(ordinal) ? -1 : reader.GetInt32(ordinal);
        }

        public static long GetSequenceNumber(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("SequenceNumber");
            return reader.IsDBNull(ordinal) ? -1 : reader.GetInt64(ordinal);
        }

        public static Guid? GetPayloadId(this DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("PayloadID");
            return reader.IsDBNull(ordinal) ? (Guid?)null : reader.GetGuid(ordinal);
        }

        public static SemanticVersion GetVersion(DbDataReader reader)
        {
            string versionString = reader.GetString("SemanticVersion");
            return SemanticVersion.Parse(versionString);
        }

        internal static SqlString GetReason(HistoryEvent historyEvent)
        {
            return historyEvent.EventType switch
            {
                EventType.SubOrchestrationInstanceFailed => ((SubOrchestrationInstanceFailedEvent)historyEvent).Reason,
                EventType.TaskFailed => ((TaskFailedEvent)historyEvent).Reason,
                _ => SqlString.Null,
            };
        }

        static string? GetReason(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Reason");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        internal static SqlString GetPayloadText(HistoryEvent e)
        {
            DTUtils.TryGetPayloadText(e, out string? payloadText);
            return payloadText ?? SqlString.Null;
        }

        static string? GetPayloadText(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("PayloadText");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        static string GetInstanceId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("InstanceID");
            return reader.GetString(ordinal);
        }

        internal static string? GetExecutionId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("ExecutionID");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        static DateTime GetVisibleTime(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("VisibleTime");
            return GetUtcDateTime(reader, ordinal);
        }

        static DateTime GetTimestamp(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Timestamp");
            return GetUtcDateTime(reader, ordinal);
        }

        static DateTime? GetUtcDateTimeOrNull(this DbDataReader reader, int columnIndex)
        {
            return reader.IsDBNull(columnIndex) ? (DateTime?)null : GetUtcDateTime(reader, columnIndex);
        }

        static DateTime GetUtcDateTime(DbDataReader reader, int ordinal)
        {
            // The SQL client always assumes DateTimeKind.Unspecified. We need to modify the result so that it knows it is UTC.
            return DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);
        }

        public static Task<DbDataReader> ExecuteReaderAsync(
            DbCommand command,
            LogHelper traceHelper,
            CancellationToken cancellationToken = default)
        {
            return ExecuteSprocAndTraceAsync(command, traceHelper, cmd => cmd.ExecuteReaderAsync(cancellationToken));
        }

        public static Task<int> ExecuteNonQueryAsync(
            DbCommand command,
            LogHelper traceHelper,
            CancellationToken cancellationToken = default)
        {
            return ExecuteSprocAndTraceAsync(command, traceHelper, cmd => cmd.ExecuteNonQueryAsync(cancellationToken));
        }

        static async Task<T> ExecuteSprocAndTraceAsync<T>(
            DbCommand command,
            LogHelper traceHelper,
            Func<DbCommand, Task<T>> executor)
        {
            var latencyStopwatch = Stopwatch.StartNew();
            try
            {
                return await executor(command);
            }
            finally
            {
                latencyStopwatch.Stop();
                traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
            }
        }

        public static bool IsUniqueKeyViolation(SqlException exception)
        {
            return exception.Errors.Cast<SqlError>().Any(e => e.Class == 14 && (e.Number == 2601 || e.Number == 2627));
        }

        public static void SetDateTime(this SqlDataRecord record, int ordinal, DateTime? dateTime)
        {
            if (dateTime.HasValue)
            {
                record.SetDateTime(ordinal, dateTime.Value);
            }
            else
            {
                record.SetDBNull(ordinal);
            }
        }
    }
}
