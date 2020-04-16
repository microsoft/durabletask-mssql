using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
using DurableTask.Core;
using DurableTask.Core.History;
using Microsoft.SqlServer.Server;

namespace DurableTask.RelationalDb
{
    // TODO: Refactor this file
    // TODO: Separate out SQL Server-specific APIs
    static class SqlUtils
    {
        static readonly SqlMetaData[] TaskEventSchema = new SqlMetaData[]
        {
            new SqlMetaData("SequenceNumber", SqlDbType.BigInt),
            new SqlMetaData("VisibleTime", SqlDbType.DateTime),
            new SqlMetaData("InstanceID", SqlDbType.NVarChar, 100),
            new SqlMetaData("ExecutionID", SqlDbType.NVarChar, 100),
            new SqlMetaData("EventType", SqlDbType.VarChar, 30),
            new SqlMetaData("Name", SqlDbType.NVarChar, 256),
            new SqlMetaData("RuntimeStatus", SqlDbType.VarChar, 30),
            new SqlMetaData("TaskID", SqlDbType.Int),
            new SqlMetaData("Reason", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("PayloadText", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("CustomStatusText", SqlDbType.NVarChar, -1 /* max */),
            new SqlMetaData("IsPlayed", SqlDbType.Bit),
            new SqlMetaData("LockedBy", SqlDbType.NVarChar, 100),
            new SqlMetaData("LockExpiration", SqlDbType.DateTime2),
            new SqlMetaData("CompletedTime", SqlDbType.DateTime2),
        };

        public static void AddParameter(this IDbCommand command, string name, object value)
        {
            IDbDataParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            command.Parameters.Add(parameter);
        }

        public static DateTime? GetDateTimeOrNull(this DbDataReader reader, int columnIndex)
        {
            return reader.IsDBNull(columnIndex) ? (DateTime?)null : reader.GetDateTime(columnIndex);
        }

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
                        FireAt = (DateTime)reader["VisibleTime"],
                    };
                    break;
                case EventType.TimerFired:
                    historyEvent = new TimerFiredEvent(eventId)
                    {
                        FireAt = (DateTime)reader["VisibleTime"],
                        TimerId = GetTaskId(reader),
                    };
                    break;
                default:
                    throw new InvalidOperationException($"Don't know how to interpret '{eventType}'.");
            }

            historyEvent.Timestamp = (DateTime)reader["Timestamp"];
            historyEvent.IsPlayed = isOrchestrationHistory && (bool)reader["IsPlayed"];
            return historyEvent;
        }

        public static IEnumerable<SqlDataRecord>? ToTableValueParameter(
            this IEnumerable<TaskMessage> messages,
            bool onlyMessageId = false)
        {
            if (!messages.Any())
            {
                return null;
            }

            var record = new SqlDataRecord(TaskEventSchema);
            return messages.Select(msg => msg.Populate(record));
        }

        public static IEnumerable<SqlDataRecord> ToTableValueParameter(
            this TaskMessage taskMessage,
            bool onlyMessageId = false)
        {
            return new[] { taskMessage.Populate(new SqlDataRecord(TaskEventSchema), onlyMessageId) };
        }

        public static IEnumerable<SqlDataRecord>? ToTableValueParameter(
            this IEnumerable<DbTaskEvent> events)
        {
            if (!events.Any())
            {
                return null;
            }

            var record = new SqlDataRecord(TaskEventSchema);
            return events.Select(e => e.Populate(record));
        }

        public static SqlDataRecord Populate(
            this DbTaskEvent taskEvent,
            SqlDataRecord record)
        {
            taskEvent.Message.Populate(record);
            record.SetSqlString(TaskEventFields.LockedBy, taskEvent.LockedBy ?? SqlString.Null);
            record.SetSqlDateTime(TaskEventFields.LockExpiration, taskEvent.LockExpiration ?? SqlDateTime.Null);
            return record;
        }

        public static SqlDataRecord Populate(
            this TaskMessage taskMessage,
            SqlDataRecord record,
            bool onlyMessageId = false)
        {
            record.SetSqlInt64(TaskEventFields.SequenceNumber, taskMessage.SequenceNumber);
            record.SetSqlString(TaskEventFields.InstanceID, taskMessage.OrchestrationInstance.InstanceId);

            if (!onlyMessageId)
            {
                record.SetSqlDateTime(TaskEventFields.VisibleTime, GetVisibleTime(taskMessage.Event));
                record.SetSqlString(TaskEventFields.ExecutionID, taskMessage.OrchestrationInstance.ExecutionId);
                record.SetSqlString(TaskEventFields.EventType, taskMessage.Event.EventType.ToString());
                record.SetSqlString(TaskEventFields.Name, GetName(taskMessage.Event));
                record.SetSqlString(TaskEventFields.RuntimeStatus, GetRuntimeStatus(taskMessage.Event));
                record.SetSqlInt32(TaskEventFields.TaskID, GetTaskId(taskMessage.Event));
                record.SetSqlString(TaskEventFields.Reason, GetReason(taskMessage.Event));
                record.SetSqlString(TaskEventFields.PayloadText, GetPayloadText(taskMessage.Event));
            }

            return record;
        }

        public static IEnumerable<SqlDataRecord>? ToTableValueParameter(
            this IEnumerable<HistoryEvent> newEvents,
            OrchestrationInstance instance,
            int nextSequenceNumber)
        {
            if (!newEvents.Any())
            {
                return null;
            }

            var record = new SqlDataRecord(TaskEventSchema);
            return newEvents.Select(e => e.Populate(instance, nextSequenceNumber++, record));
        }

        public static SqlDataRecord Populate(this HistoryEvent e, OrchestrationInstance instance, int sequenceNumber, SqlDataRecord record)
        {
            record.SetSqlDateTime(TaskEventFields.VisibleTime, GetVisibleTime(e));
            record.SetSqlString(TaskEventFields.InstanceID, instance.InstanceId);
            record.SetSqlString(TaskEventFields.ExecutionID, instance.ExecutionId);
            record.SetSqlString(TaskEventFields.EventType, e.EventType.ToString());
            record.SetSqlString(TaskEventFields.Name, GetName(e));
            record.SetSqlString(TaskEventFields.RuntimeStatus, GetRuntimeStatus(e));
            record.SetSqlInt32(TaskEventFields.TaskID, GetTaskId(e));
            record.SetSqlString(TaskEventFields.Reason, GetReason(e));
            record.SetSqlString(TaskEventFields.PayloadText, GetPayloadText(e));
            record.SetSqlInt64(TaskEventFields.SequenceNumber, sequenceNumber);
            record.SetBoolean(TaskEventFields.IsPlayed, e.IsPlayed);
            return record;
        }

        public static IEnumerable<SqlDataRecord> ToTableValueParameter(
            this OrchestrationState orchestrationState,
            DateTime? lockExpiration)
        {
            var record = new SqlDataRecord(TaskEventSchema);
            record.SetSqlString(TaskEventFields.InstanceID, orchestrationState.OrchestrationInstance.InstanceId);
            record.SetSqlString(TaskEventFields.ExecutionID, orchestrationState.OrchestrationInstance.ExecutionId);
            record.SetSqlString(TaskEventFields.CustomStatusText, StripJsonNulls(orchestrationState.Status));
            record.SetSqlString(TaskEventFields.RuntimeStatus, orchestrationState.OrchestrationStatus.ToString());
            record.SetSqlString(TaskEventFields.PayloadText, StripJsonNulls(orchestrationState.Output));
            record.SetSqlDateTime(TaskEventFields.LockExpiration, lockExpiration ?? SqlDateTime.Null);
            record.SetSqlDateTime(TaskEventFields.CompletedTime, orchestrationState.CompletedTime > default(DateTime) ? orchestrationState.CompletedTime : SqlDateTime.Null);
            return new[] { record };
        }

        public static OrchestrationState GetOrchestrationState(this DbDataReader reader)
        {
            return new OrchestrationState
            {
                CompletedTime = reader.GetDateTimeOrNull(reader.GetOrdinal("CompletedTime")) ?? default,
                CreatedTime = reader.GetDateTimeOrNull(reader.GetOrdinal("CreatedTime")) ?? default,
                Input = StripJsonNulls(reader.GetStringOrNull(reader.GetOrdinal("InputText"))),
                LastUpdatedTime = reader.GetDateTimeOrNull(reader.GetOrdinal("LastUpdatedTime")) ?? default,
                Name = GetName(reader),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetInstanceId(reader),
                    ExecutionId = GetExecutionId(reader),
                },
                OrchestrationStatus = (OrchestrationStatus)Enum.Parse(
                    typeof(OrchestrationStatus),
                    GetStringOrNull(reader, reader.GetOrdinal("RuntimeStatus"))),
                Output = StripJsonNulls(GetStringOrNull(reader, reader.GetOrdinal("OutputText"))),
                Status = GetStringOrNull(reader, reader.GetOrdinal("CustomStatusText")),
            };
        }

        static string? StripJsonNulls(string? value)
        {
            if (value == null || string.Equals(value, "null"))
            {
                return null;
            }

            return value;
        }

        static SqlDateTime GetVisibleTime(HistoryEvent historyEvent)
        {
            return historyEvent.EventType switch
            {
                EventType.TimerCreated => ((TimerCreatedEvent)historyEvent).FireAt,
                EventType.TimerFired => ((TimerFiredEvent)historyEvent).FireAt,
                _ => SqlDateTime.Null,
            };
        }

        static SqlString GetRuntimeStatus(HistoryEvent historyEvent)
        {
            return DTUtils.GetRuntimeStatus(historyEvent)?.ToString() ?? SqlString.Null;
        }

        static SqlString GetName(HistoryEvent historyEvent)
        {
            return DTUtils.GetName(historyEvent) ?? SqlString.Null;
        }

        static string? GetName(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Name");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        static SqlInt32 GetTaskId(HistoryEvent historyEvent)
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

        static SqlString GetReason(HistoryEvent historyEvent)
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

        static SqlString GetPayloadText(HistoryEvent e)
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

        static string? GetExecutionId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("ExecutionID");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        static class TaskEventFields
        {
            // NOTE: These numbers must be kept in sync with the TaskEvent SQL type
            // NOTE: Also, these must be kept in sync with the static TaskEventSchema above
            public const int SequenceNumber = 0;
            public const int VisibleTime = 1;
            public const int InstanceID = 2;
            public const int ExecutionID = 3;
            public const int EventType = 4;
            public const int Name = 5;
            public const int RuntimeStatus = 6;
            public const int TaskID = 7;
            public const int Reason = 8;
            public const int PayloadText = 9;
            public const int CustomStatusText = 10;
            public const int IsPlayed = 11;
            public const int LockedBy = 12;
            public const int LockExpiration = 13;
            public const int CompletedTime = 14;
        }
    }
}
