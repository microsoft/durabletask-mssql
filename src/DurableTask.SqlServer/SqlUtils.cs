// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlTypes;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;
    using SemVersion;

    static class SqlUtils
    {
        static readonly Random random = new Random();
        static readonly char[] TraceContextSeparators = new char[] { '\n' };

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
            switch (eventType)
            {
                case EventType.ContinueAsNew:
                    historyEvent = new ContinueAsNewEvent(eventId, GetPayloadText(reader));
                    break;
                case EventType.EventRaised:
                    historyEvent = new EventRaisedEvent(eventId, GetPayloadText(reader))
                    {
                        Name = GetName(reader),
                        ParentTraceContext = GetTraceContext(reader),
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
                    TryGetFailureDetails(reader, out FailureDetails? executionFailedDetails);
                    historyEvent = new ExecutionCompletedEvent(
                        eventId,
                        result: GetPayloadText(reader),
                        orchestrationStatus: GetRuntimeStatus(reader),
                        failureDetails: executionFailedDetails);
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
                        Tags = null, // TODO
                        Version = GetVersion(reader),
                        ParentTraceContext = GetTraceContext(reader),
                    };
                    string? parentInstanceId = GetParentInstanceId(reader);
                    if (parentInstanceId != null)
                    {
                        ((ExecutionStartedEvent)historyEvent).ParentInstance = new ParentInstance
                        {
                            OrchestrationInstance = new OrchestrationInstance
                            {
                                InstanceId = parentInstanceId
                            },
                            TaskScheduleId = GetTaskId(reader)
                        };
                    }
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
                    historyEvent = new SubOrchestrationInstanceCompletedEvent(eventId: -1, GetTaskId(reader), GetPayloadText(reader));
                    break;
                case EventType.SubOrchestrationInstanceCreated:
                    historyEvent = new SubOrchestrationInstanceCreatedEvent(eventId)
                    {
                        Input = GetPayloadText(reader),
                        InstanceId = "", // Placeholder - shouldn't technically be needed (adding it requires a SQL schema change)
                        Name = GetName(reader),
                        Version = null,
                    };
                    break;
                case EventType.SubOrchestrationInstanceFailed:
                    string? subOrchFailedReason = null;
                    string? subOrchFailedDetails = null;
                    if (!TryGetFailureDetails(reader, out FailureDetails? subOrchFailureDetails))
                    {
                        // Fall back to the old behavior
                        subOrchFailedReason = GetReason(reader);
                        subOrchFailedDetails = GetPayloadText(reader);
                    }

                    historyEvent = new SubOrchestrationInstanceFailedEvent(
                        eventId: -1,
                        taskScheduledId: GetTaskId(reader),
                        reason: subOrchFailedReason,
                        details: subOrchFailedDetails,
                        failureDetails: subOrchFailureDetails);
                    break;
                case EventType.TaskCompleted:
                    historyEvent = new TaskCompletedEvent(
                        eventId: -1,
                        taskScheduledId: GetTaskId(reader),
                        result: GetPayloadText(reader));
                    break;
                case EventType.TaskFailed:
                    string? taskFailedReason = null;
                    string? taskFailedDetails = null;
                    if (!TryGetFailureDetails(reader, out FailureDetails? taskFailureDetails))
                    {
                        // Fall back to the old behavior
                        taskFailedReason = GetReason(reader);
                        taskFailedDetails = GetPayloadText(reader);
                    }

                    historyEvent = new TaskFailedEvent(
                        eventId: -1,
                        taskScheduledId: GetTaskId(reader),
                        reason: taskFailedReason,
                        details: taskFailedDetails,
                        failureDetails: taskFailureDetails);
                    break;
                case EventType.TaskScheduled:
                    historyEvent = new TaskScheduledEvent(eventId)
                    {
                        Input = GetPayloadText(reader),
                        Name = GetName(reader),
                        Version = GetVersion(reader),
                        ParentTraceContext = GetTraceContext(reader),
                    };
                    break;
                case EventType.TimerCreated:
                    historyEvent = new TimerCreatedEvent(eventId)
                    {
                        FireAt = GetVisibleTime(reader),
                    };
                    break;
                case EventType.TimerFired:
                    historyEvent = new TimerFiredEvent(eventId: -1)
                    {
                        FireAt = GetVisibleTime(reader),
                        TimerId = GetTaskId(reader),
                    };
                    break;
                case EventType.ExecutionSuspended:
                    historyEvent = new ExecutionSuspendedEvent(eventId, GetPayloadText(reader));
                    break;
                case EventType.ExecutionResumed:
                    historyEvent = new ExecutionResumedEvent(eventId, GetPayloadText(reader));
                    break;
                default:
                    throw new InvalidOperationException($"Don't know how to interpret '{eventType}'.");
            }

            historyEvent.Timestamp = GetTimestamp(reader);
            historyEvent.IsPlayed = isOrchestrationHistory && (bool)reader["IsPlayed"];
            return historyEvent;
        }

        static bool TryGetFailureDetails(DbDataReader reader, out FailureDetails? details)
        {
            // Failure details are expected to be in JSON format. In previous versions, it was just an error message
            // that isn't expected to be JSON.
            string? text = GetPayloadText(reader);
            if (string.IsNullOrEmpty(text) || text![0] != '{')
            {
                details = null;
                return false;
            }

            return DTUtils.TryDeserializeFailureDetails(text, out details);
        }

        public static OrchestrationState GetOrchestrationState(this DbDataReader reader)
        {
            ParentInstance? parentInstance = null;
            string? parentInstanceId = GetParentInstanceId(reader);
            if (parentInstanceId != null)
            {
                parentInstance = new ParentInstance
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = parentInstanceId
                    }
                };
            }

            var state = new OrchestrationState
            {
                CompletedTime = GetUtcDateTime(reader, "CompletedTime"),
                CreatedTime = GetUtcDateTime(reader, "CreatedTime"),
                Input = reader.GetStringOrNull(reader.GetOrdinal("InputText")),
                LastUpdatedTime = GetUtcDateTime(reader, "LastUpdatedTime"),
                Name = GetName(reader),
                Version = GetVersion(reader),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetInstanceId(reader),
                    ExecutionId = GetExecutionId(reader),
                },
                OrchestrationStatus = GetRuntimeStatus(reader),
                Status = GetStringOrNull(reader, reader.GetOrdinal("CustomStatusText")),
                ParentInstance = parentInstance
            };

            // The OutputText column is overloaded to contain either orchestration output or failure details
            // if the task hub worker is configured to use legacy error propagation.
            string? rawOutput = GetStringOrNull(reader, reader.GetOrdinal("OutputText"));
            if (rawOutput != null)
            {
                if (state.OrchestrationStatus == OrchestrationStatus.Failed &&
                    DTUtils.TryDeserializeFailureDetails(rawOutput, out FailureDetails? failureDetails))
                {
                    state.FailureDetails = failureDetails;
                }
                else
                {
                    state.Output = rawOutput;
                }
            }

            return state;
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

        public static SqlString GetParentInstanceId(HistoryEvent historyEvent)
        {
            return DTUtils.GetParentInstanceId(historyEvent) ?? SqlString.Null;
        }

        public static string? GetParentInstanceId(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("ParentInstanceID");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        public static OrchestrationStatus GetRuntimeStatus(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("RuntimeStatus");
            return (OrchestrationStatus)Enum.Parse(typeof(OrchestrationStatus), GetStringOrNull(reader, ordinal));
        }

        public static Guid? GetPayloadId(this DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("PayloadID");
            return reader.IsDBNull(ordinal) ? (Guid?)null : reader.GetGuid(ordinal);
        }

        public static SemanticVersion GetSemanticVersion(DbDataReader reader)
        {
            string versionString = reader.GetString("SemanticVersion");
            return SemanticVersion.Parse(versionString);
        }

        public static SqlString GetVersion(HistoryEvent historyEvent)
        {
            return DTUtils.GetVersion(historyEvent) ?? SqlString.Null;
        }

        public static string? GetVersion(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("Version");
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
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

        internal static string GetInstanceId(DbDataReader reader)
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
            return GetUtcDateTime(reader, "VisibleTime");
        }

        static DateTime GetTimestamp(DbDataReader reader)
        {
            return GetUtcDateTime(reader, "Timestamp");
        }

        static DateTime GetUtcDateTime(DbDataReader reader, string columnName)
        {
            int ordinal = reader.GetOrdinal(columnName);
            return GetUtcDateTime(reader, ordinal);
        }

        static DateTime GetUtcDateTime(DbDataReader reader, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
            {
                // Note that some serializers (like protobuf) won't accept non-UTC DateTime objects.
                return DateTime.SpecifyKind(default, DateTimeKind.Utc);
            }

            // The SQL client always assumes DateTimeKind.Unspecified. We need to modify the result so that it knows it is UTC.
            return DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);
        }

        internal static SqlString GetTraceContext(HistoryEvent e)
        {
            if (e is not ISupportsDurableTraceContext eventWithTraceContext ||
                eventWithTraceContext.ParentTraceContext == null)
            {
                return SqlString.Null;
            }

            DistributedTraceContext traceContext = eventWithTraceContext.ParentTraceContext;

            // We prefer a simple format instead of JSON because external callers may interact with this
            // data and we don't want to expose them to some internal JSON serialization format.
            var sb = new StringBuilder(traceContext.TraceParent, capacity: 800);
            if (!string.IsNullOrEmpty(traceContext.TraceState))
            {
                sb.Append('\n').Append(traceContext.TraceState);
            }

            return sb.ToString();
        }

        static DistributedTraceContext? GetTraceContext(DbDataReader reader)
        {
            int ordinal = reader.GetOrdinal("TraceContext");
            if (reader.IsDBNull(ordinal))
            {
                return null;
            }

            string text = reader.GetString(ordinal);
            if (string.IsNullOrEmpty(text))
            {
                return null;
            }

            string[] parts = text.Split(TraceContextSeparators, count: 2, StringSplitOptions.RemoveEmptyEntries);
            var traceContext = new DistributedTraceContext(traceParent: parts[0]);

            if (parts.Length > 1)
            {
                traceContext.TraceState = parts[1];
            }

            traceContext.ActivityStartTime = GetTimestamp(reader);
            return traceContext;
        }

        public static SqlParameter AddInstanceIDsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            IEnumerable<string> instanceIds,
            string schemaName)
        {
            static IEnumerable<SqlDataRecord> GetInstanceIdRecords(IEnumerable<string> instanceIds)
            {
                var record = new SqlDataRecord(new SqlMetaData("InstanceID", SqlDbType.VarChar, maxLength: 100));
                foreach (string instanceId in instanceIds)
                {
                    record.SetString(0, instanceId);
                    yield return record;
                }
            }

            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = $"{schemaName}.InstanceIDs";
            param.Value = instanceIds.Any() ? GetInstanceIdRecords(instanceIds) : null;
            return param;
        }

        public static Task<DbDataReader> ExecuteReaderAsync(
            DbCommand command,
            LogHelper traceHelper,
            string? instanceId = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteSprocAndTraceAsync(
                command,
                traceHelper,
                instanceId,
                cmd => cmd.ExecuteReaderAsync(cancellationToken));
        }

        public static Task<int> ExecuteNonQueryAsync(
            DbCommand command,
            LogHelper traceHelper,
            string? instanceId = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteSprocAndTraceAsync(
                command,
                traceHelper,
                instanceId,
                cmd => cmd.ExecuteNonQueryAsync(cancellationToken));
        }

        public static Task<object> ExecuteScalarAsync(
            DbCommand command,
            LogHelper traceHelper,
            string? instanceId = null,
            CancellationToken cancellationToken = default)
        {
            return ExecuteSprocAndTraceAsync(
                command,
                traceHelper,
                instanceId,
                cmd => cmd.ExecuteScalarAsync(cancellationToken));
        }

        static async Task<T> ExecuteSprocAndTraceAsync<T>(
            DbCommand command,
            LogHelper traceHelper,
            string? instanceId,
            Func<DbCommand, Task<T>> executor)
        {
            var context = new SprocExecutionContext();
            try
            {
                return await WithRetry(command, executor, context, traceHelper, instanceId);
            }
            finally
            {
                context.LatencyStopwatch.Stop();
                switch (command.CommandType)
                {
                    case CommandType.StoredProcedure:
                        traceHelper.SprocCompleted(command.CommandText, context.LatencyStopwatch, context.RetryCount, instanceId);
                        break;
                    default:
                        traceHelper.CommandCompleted(command.CommandText, context.LatencyStopwatch, context.RetryCount, instanceId);
                        break;
                }
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

        public static DateTime ToSqlUtcDateTime(this DateTime dateTime, DateTime defaultValue)
        {
            if (dateTime == default)
            {
                return defaultValue;
            }
            else if (dateTime.Kind == DateTimeKind.Utc)
            {
                return dateTime;
            }
            else
            {
                return dateTime.ToUniversalTime();
            }
        }

        static async Task<T> WithRetry<T>(DbCommand command, Func<DbCommand, Task<T>> executor, SprocExecutionContext context, LogHelper traceHelper, string? instanceId, int maxRetries = 5)
        {
            context.RetryCount = 0;

            while (true)
            {
                try
                {
                    // Open connection if network blip caused it to close on a previous attempt
                    if (command.Connection.State != ConnectionState.Open)
                    {
                        await command.Connection.OpenAsync();
                    }
                    return await executor(command);
                }
                catch (Exception e)
                {
                    if (!IsTransient(e))
                    {
                        // Not a retriable exception
                        throw;
                    }

                    if (context.RetryCount >= maxRetries)
                    {
                        // Maxed out on retries. The layer above may do its own retries later.
                        throw;
                    }

                    // Linear backoff where we add 1 second each time, so for retryCount = 5
                    // we could delay as long as 0 + 1 + 2 + 3 + 4 = 10 total seconds.
                    TimeSpan delay = TimeSpan.FromSeconds(context.RetryCount);
                    lock (random)
                    {
                        // Add a small amount of random delay to distribute concurrent retries 
                        delay += TimeSpan.FromMilliseconds(random.Next(100));
                    }

                    // Log a warning so that these issues can be properly investigated
                    traceHelper.TransientDatabaseFailure(e, instanceId, context.RetryCount);

                    await Task.Delay(delay);

                    context.RetryCount++;
                }
            }
        }

        static bool IsTransient(Exception exception)
        {
            if (exception is SqlException sqlException)
            {
                foreach (SqlError error in sqlException.Errors)
                {
                    switch (error.Number)
                    {
                        // SQL Error Code: 49920
                        // Cannot process request. Too many operations in progress for subscription "%ld".
                        // The service is busy processing multiple requests for this subscription.
                        // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for operation status.
                        // Wait until pending requests are complete or delete one of your pending requests and retry your request later.
                        case 49920:
                        // SQL Error Code: 49919
                        // Cannot process create or update request. Too many create or update operations in progress for subscription "%ld".
                        // The service is busy processing multiple create or update requests for your subscription or server.
                        // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for pending operations.
                        // Wait till pending create or update requests are complete or delete one of your pending requests and
                        // retry your request later.
                        case 49919:
                        // SQL Error Code: 49918
                        // Cannot process request. Not enough resources to process request.
                        // The service is currently busy. Please retry the request later.
                        case 49918:
                        // SQL Error Code: 41839
                        // Transaction exceeded the maximum number of commit dependencies.
                        case 41839:
                        // SQL Error Code: 41325
                        // The current transaction failed to commit due to a serializable validation failure.
                        case 41325:
                        // SQL Error Code: 41305
                        // The current transaction failed to commit due to a repeatable read validation failure.
                        case 41305:
                        // SQL Error Code: 41302
                        // The current transaction attempted to update a record that has been updated since the transaction started.
                        case 41302:
                        // SQL Error Code: 41301
                        // Dependency failure: a dependency was taken on another transaction that later failed to commit.
                        case 41301:
                        // SQL Error Code: 40613
                        // Database XXXX on server YYYY is not currently available. Please retry the connection later.
                        // If the problem persists, contact customer support, and provide them the session tracing ID of ZZZZZ.
                        case 40613:
                        // SQL Error Code: 40501
                        // The service is currently busy. Retry the request after 10 seconds. Code: (reason code to be decoded).
                        case 40501:
                        // SQL Error Code: 40197
                        // The service has encountered an error processing your request. Please try again.
                        case 40197:
                        // SQL Error Code: 10936
                        // Resource ID : %d. The request limit for the elastic pool is %d and has been reached.
                        // See 'http://go.microsoft.com/fwlink/?LinkId=267637' for assistance.
                        case 10936:
                        // SQL Error Code: 10929
                        // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database is %d.
                        // However, the server is currently too busy to support requests greater than %d for this database.
                        // For more information, see http://go.microsoft.com/fwlink/?LinkId=267637. Otherwise, please try again.
                        case 10929:
                        // SQL Error Code: 10928
                        // Resource ID: %d. The %s limit for the database is %d and has been reached. For more information,
                        // see http://go.microsoft.com/fwlink/?LinkId=267637.
                        case 10928:
                        // SQL Error Code: 10060
                        // A network-related or instance-specific error occurred while establishing a connection to SQL Server.
                        // The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server
                        // is configured to allow remote connections. (provider: TCP Provider, error: 0 - A connection attempt failed
                        // because the connected party did not properly respond after a period of time, or established connection failed
                        // because connected host has failed to respond.)"}
                        case 10060:
                        // SQL Error Code: 10054
                        // A transport-level error has occurred when sending the request to the server.
                        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
                        case 10054:
                        // SQL Error Code: 10053
                        // A transport-level error has occurred when receiving results from the server.
                        // An established connection was aborted by the software in your host machine.
                        case 10053:
                        // SQL Error Code: 1205
                        // Deadlock
                        case 1205:
                        // SQL Error Code: 233
                        // The client was unable to establish a connection because of an error during connection initialization process before login.
                        // Possible causes include the following: the client tried to connect to an unsupported version of SQL Server;
                        // the server was too busy to accept new connections; or there was a resource limitation (insufficient memory or maximum
                        // allowed connections) on the server. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by
                        // the remote host.)
                        case 233:
                        // SQL Error Code: 121
                        // The semaphore timeout period has expired
                        case 121:
                        // SQL Error Code: 64
                        // A connection was successfully established with the server, but then an error occurred during the login process.
                        // (provider: TCP Provider, error: 0 - The specified network name is no longer available.)
                        case 64:
                        // DBNETLIB Error Code: 20
                        // The instance of SQL Server you attempted to connect to does not support encryption.
                        case 20:
                            return true;
                            // This exception can be thrown even if the operation completed successfully, so it's safer to let the application fail.
                            // DBNETLIB Error Code: -2
                            // Timeout expired. The timeout period elapsed prior to completion of the operation or the server is not responding. The statement has been terminated.
                            //case -2:
                    }
                }

                return false;
            }

            return exception is TimeoutException;
        }

        class SprocExecutionContext
        {
            public Stopwatch LatencyStopwatch { get; } = Stopwatch.StartNew();

            public int RetryCount { get; set; }
        }
    }
}
