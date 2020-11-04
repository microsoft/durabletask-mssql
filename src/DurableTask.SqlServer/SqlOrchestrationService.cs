namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlTypes;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.SqlServer.SqlTypes;
    using DurableTask.SqlServer.Utils;
    using Microsoft.Data.SqlClient;

    public class SqlOrchestrationService : OrchestrationServiceBase
    {
        readonly BackoffPollingHelper orchestrationBackoffHelper = new BackoffPollingHelper(
            minimumInterval: TimeSpan.FromMilliseconds(50),
            maximumInterval: TimeSpan.FromSeconds(3)); // TODO: Configurable

        readonly BackoffPollingHelper activityBackoffHelper = new BackoffPollingHelper(
            minimumInterval: TimeSpan.FromMilliseconds(50),
            maximumInterval: TimeSpan.FromSeconds(3)); // TODO: Configurable

        readonly SqlProviderOptions options;
        readonly LogHelper traceHelper;
        readonly SqlDbManager dbManager;

        public SqlOrchestrationService(SqlProviderOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.traceHelper = new LogHelper(this.options.LoggerFactory.CreateLogger("DurableTask.SqlServer"));
            this.dbManager = new SqlDbManager(this.options, this.traceHelper);
        }

        async Task<SqlConnection> GetAndOpenConnectionAsync(CancellationToken cancelToken = default)
        {
            SqlConnection connection = this.options.CreateConnection();
            await connection.OpenAsync(cancelToken);
            return connection;
        }

        SqlCommand GetSprocCommand(SqlConnection connection, string sprocName)
        {
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = sprocName;
            return command;
        }

        string GetLockedByValue()
        {
            // TODO: Configurable
            return $"{Environment.MachineName}||{this.options.AppName}";
        }

        public override Task CreateAsync(bool recreateInstanceStore)
            => this.dbManager.CreateOrUpgradeSchemaAsync(recreateInstanceStore);

        public override Task CreateIfNotExistsAsync()
            => this.CreateAsync(recreateInstanceStore: false);

        public override Task DeleteAsync(bool deleteInstanceStore)
        {
            if (!deleteInstanceStore)
            {
                throw new NotSupportedException($"Setting the {deleteInstanceStore} parameter to false is not supported.");
            }

            return this.dbManager.DeleteSchemaAsync();
        }

        public override async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
                using SqlCommand command = this.GetSprocCommand(connection, "dt._LockNextOrchestration");

                int batchSize = 10; // TODO: Make configurable
                DateTime lockExpiration = DateTime.UtcNow.Add(this.options.TaskEventLockTimeout);

                command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
                command.Parameters.Add("@LockedBy", SqlDbType.VarChar, 100).Value = this.GetLockedByValue();
                command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

                DbDataReader reader;

                try
                {
                    reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper, cancellationToken);
                }
                catch (Exception e)
                {
                    this.traceHelper.ProcessingError(e, new OrchestrationInstance());
                    throw;
                }

                using (reader)
                {
                    // Result #1: The list of control queue messages
                    int longestWaitTime = 0;
                    var messages = new List<TaskMessage>(capacity: batchSize);
                    var eventPayloadMappings = new Dictionary<HistoryEvent, Guid>(capacity: batchSize);
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        TaskMessage message = reader.GetTaskMessage();
                        messages.Add(message);
                        Guid? payloadId = reader.GetPayloadId();
                        if (payloadId.HasValue)
                        {
                            // TODO: Need to understand what the payload behavior is for retry events
                            eventPayloadMappings.Add(message.Event, payloadId.Value);
                        }

                        longestWaitTime = Math.Max(longestWaitTime, reader.GetInt32("WaitTime"));
                    }

                    if (messages.Count == 0)
                    {
                        // TODO: Make this dynamic based on the number of readers
                        await this.orchestrationBackoffHelper.WaitAsync(cancellationToken);
                        continue;
                    }

                    this.orchestrationBackoffHelper.Reset();

                    // Result #2: The full event history for the locked instance
                    var history = new List<HistoryEvent>();
                    if (await reader.NextResultAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            history.Add(reader.GetHistoryEvent(isOrchestrationHistory: true));
                        }
                    }
                    else
                    {
                        this.traceHelper.GenericWarning(
                            details: "Failed to read history from the database!",
                            instanceId: messages.FirstOrDefault(m => m.OrchestrationInstance?.InstanceId != null)?.OrchestrationInstance.InstanceId);
                    }

                    var runtimeState = new OrchestrationRuntimeState(history);

                    string orchestrationName;
                    OrchestrationInstance instance;
                    if (runtimeState.ExecutionStartedEvent != null)
                    {
                        // This is an existing instance
                        orchestrationName = runtimeState.Name;
                        instance = runtimeState.OrchestrationInstance;
                    }
                    else if (messages[0].Event is ExecutionStartedEvent startedEvent)
                    {
                        // This is a new instance
                        orchestrationName = startedEvent.Name;
                        instance = startedEvent.OrchestrationInstance;
                    }
                    else
                    {
                        // Don't know what to do with this message (TODO: Need to confirm behavior)
                        orchestrationName = "(Unknown)";
                        instance = new OrchestrationInstance();
                    }

                    return new ExtendedOrchestrationWorkItem(orchestrationName, instance)
                    {
                        InstanceId = messages[0].OrchestrationInstance.InstanceId,
                        LockedUntilUtc = lockExpiration,
                        NewMessages = messages,
                        OrchestrationRuntimeState = runtimeState,
                        EventPayloadMappings = eventPayloadMappings,
                    };
                }
            }
        }

        public override Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public override async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            ExtendedOrchestrationWorkItem currentWorkItem = (ExtendedOrchestrationWorkItem)workItem;

            this.traceHelper.CheckpointingOrchestration(orchestrationState);

            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetSprocCommand(connection, "dt._CheckpointOrchestration");

            OrchestrationInstance instance = newRuntimeState.OrchestrationInstance;
            IList<HistoryEvent> newEvents = newRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = newRuntimeState.Events;
            int nextSequenceNumber = allEvents.Count - newEvents.Count;

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instance.InstanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = instance.ExecutionId;
            command.Parameters.Add("@RuntimeStatus", SqlDbType.VarChar, size: 30).Value = orchestrationState.OrchestrationStatus.ToString();
            command.Parameters.Add("@CustomStatusPayload", SqlDbType.VarChar).Value = orchestrationState.Status ?? SqlString.Null;

            command.Parameters.AddMessageIdParameter("@DeletedEvents", workItem.NewMessages);
            command.Parameters.AddHistoryEventsParameter(
                "@NewHistoryEvents",
                newEvents,
                instance,
                nextSequenceNumber,
                currentWorkItem.EventPayloadMappings);
            command.Parameters.AddOrchestrationEventsParameter(
                "@NewOrchestrationEvents",
                orchestratorMessages,
                timerMessages,
                continuedAsNewMessage);
            command.Parameters.AddTaskEventsParameter("@NewTaskEvents", outboundMessages);

            await command.ExecuteNonQueryAsync();

            // notify pollers that new messages may be available
            if (outboundMessages.Count > 0)
            {
                this.activityBackoffHelper.Reset();
            }

            if (orchestratorMessages.Count > 0 || timerMessages.Count > 0)
            {
                this.orchestrationBackoffHelper.Reset();
            }
        }

        public override Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public override async Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using SqlConnection connection = await this.GetAndOpenConnectionAsync();
                using SqlCommand command = this.GetSprocCommand(connection, "dt._LockNextTask");

                DateTime lockExpiration = DateTime.UtcNow.Add(this.options.TaskEventLockTimeout);

                command.Parameters.Add("@LockedBy", SqlDbType.VarChar, size: 100).Value = this.GetLockedByValue();
                command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

                using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync())
                {
                    await this.activityBackoffHelper.WaitAsync(cancellationToken);
                    continue;
                }

                this.activityBackoffHelper.Reset();

                TaskMessage message = reader.GetTaskMessage();
                int dequeueCount = reader.GetInt32("DequeueCount");

                // TODO: poison message handling for high dequeue counts

                return new TaskActivityWorkItem
                {
                    Id = $"{message.OrchestrationInstance.InstanceId}:{DTUtils.GetTaskEventId(message.Event):X16}",
                    TaskMessage = message,
                    LockedUntilUtc = lockExpiration,
                };
            }

            // host is shutting down
            return null;
        }

        public override Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public override async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetSprocCommand(connection, "dt._CompleteTasks");

            command.Parameters.AddMessageIdParameter("@CompletedTasks", workItem.TaskMessage);
            command.Parameters.AddTaskEventsParameter("@Results", responseMessage);

            await command.ExecuteNonQueryAsync();

            // signal the orchestration loop to poll immediately
            this.orchestrationBackoffHelper.Reset();
        }

        public override Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public override async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetSprocCommand(connection, "dt.CreateInstance");

            ExecutionStartedEvent startEvent = (ExecutionStartedEvent)creationMessage.Event;
            command.Parameters.Add("@Name", SqlDbType.VarChar, size: 300).Value = startEvent.Name;
            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = startEvent.OrchestrationInstance.InstanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = startEvent.OrchestrationInstance.ExecutionId;
            command.Parameters.Add("@InputText", SqlDbType.VarChar).Value = startEvent.Input;
            command.Parameters.Add("@StartTime", SqlDbType.DateTime2).Value = startEvent.ScheduledStartTime;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
        }

        public override async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetSprocCommand(connection, "dt._AddOrchestrationEvents");

            command.Parameters.AddOrchestrationEventsParameter("@NewOrchestrationEvents", message);

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
        }

        public override async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                timeoutCts.Token,
                cancellationToken);

            while (true)
            {
                OrchestrationState? state = await this.GetOrchestrationStateAsync(
                    instanceId,
                    executionId,
                    combinedCts.Token);

                if (state?.OrchestrationStatus == OrchestrationStatus.Completed ||
                    state?.OrchestrationStatus == OrchestrationStatus.Failed ||
                    state?.OrchestrationStatus == OrchestrationStatus.Terminated)
                {
                    return state;
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), combinedCts.Token);
                }
                catch (TaskCanceledException e)
                {
                    if (e.CancellationToken == timeoutCts.Token)
                    {
                        throw new TimeoutException($"A caller-specified timeout of {timeout} has expired, but instance '{instanceId}' is still in an {state?.OrchestrationStatus.ToString() ?? "unknown"} state.");
                    }

                    throw;
                }
            }
        }

        public override Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId)
            => this.GetOrchestrationStateAsync(instanceId, executionId, CancellationToken.None);

        async Task<OrchestrationState?> GetOrchestrationStateAsync(
            string instanceId,
            string? executionId,
            CancellationToken cancellationToken)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
            using SqlCommand command = this.GetSprocCommand(connection, "dt.QuerySingleOrchestration");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = executionId;

            using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(
                command,
                this.traceHelper,
                cancellationToken);

            if (await reader.ReadAsync(cancellationToken))
            {
                OrchestrationState state = reader.GetOrchestrationState();
                return state;
            }

            return null;
        }

        public override async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string? reason)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetSprocCommand(connection, "dt.TerminateInstance");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@Reason", SqlDbType.VarChar).Value = reason;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
        }

        public override Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        class ExtendedOrchestrationWorkItem : TaskOrchestrationWorkItem
        {
            Dictionary<HistoryEvent, Guid>? eventPayloadMappings;

            public ExtendedOrchestrationWorkItem(string name, OrchestrationInstance instance)
            {
                this.Name = name;
                this.Instance = instance;
            }

            public string Name { get; }

            public OrchestrationInstance Instance { get; }

            public Dictionary<HistoryEvent, Guid> EventPayloadMappings
            { 
                get
                {
                    if (this.eventPayloadMappings == null)
                    {
                        this.eventPayloadMappings = new Dictionary<HistoryEvent, Guid>();
                    }

                    return this.eventPayloadMappings;
                }
                set
                {
                    this.eventPayloadMappings = value;
                }
            }
        }

        class ExtendedActivityWorkItem : TaskActivityWorkItem
        {
            public ExtendedActivityWorkItem(TaskScheduledEvent scheduledEvent)
            {
                this.ScheduledEvent = scheduledEvent;
            }

            public TaskScheduledEvent ScheduledEvent { get; }
        }
    }
}
