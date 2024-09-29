﻿// Copyright (c) Microsoft Corporation.
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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Query;
    using DurableTask.SqlServer.SqlTypes;
    using DurableTask.SqlServer.Utils;
    using Microsoft.Data.SqlClient;


    public class SqlOrchestrationService : OrchestrationServiceBase
    {
        readonly SqlOrchestrationServiceSettings settings;
        readonly BackoffPollingHelper orchestrationBackoffHelper;
        readonly BackoffPollingHelper activityBackoffHelper;
        readonly LogHelper traceHelper;
        readonly SqlDbManager dbManager;
        readonly string lockedByValue;
        readonly string userId;
        string taskHubNameValue;

        public SqlOrchestrationService(SqlOrchestrationServiceSettings? settings)
        {
            this.settings = ValidateSettings(settings) ?? throw new ArgumentNullException(nameof(settings));
            this.orchestrationBackoffHelper = new BackoffPollingHelper(
                this.settings.MinOrchestrationPollingInterval,
                this.settings.MaxOrchestrationPollingInterval,
                this.settings.DeltaBackoffOrchestrationPollingInterval);
            this.activityBackoffHelper = new BackoffPollingHelper(
                this.settings.MinActivityPollingInterval,
                this.settings.MaxActivityPollingInterval,
                this.settings.DeltaBackoffActivityPollingInterval);
            this.traceHelper = new LogHelper(this.settings.LoggerFactory.CreateLogger("DurableTask.SqlServer"));
            this.dbManager = new SqlDbManager(this.settings, this.traceHelper);
            this.lockedByValue = $"{this.settings.AppName},{Process.GetCurrentProcess().Id}";
            this.userId = new SqlConnectionStringBuilder(this.settings.TaskHubConnectionString).UserID ?? string.Empty;
            this.taskHubNameValue = settings?.TaskHubName ?? SqlOrchestrationServiceSettings.DefaultTaskHubName;
        }

        public override int MaxConcurrentTaskOrchestrationWorkItems => this.settings.MaxActiveOrchestrations;

        public override int MaxConcurrentTaskActivityWorkItems => this.settings.MaxConcurrentActivities;

        static SqlOrchestrationServiceSettings? ValidateSettings(SqlOrchestrationServiceSettings? settings)
        {
            if (settings != null)
            {
                if (string.IsNullOrEmpty(settings.TaskHubConnectionString))
                {
                    throw new ArgumentException($"A non-empty connection string value must be provided.", nameof(settings));
                }

                if (settings.WorkItemLockTimeout < TimeSpan.FromSeconds(10))
                {
                    throw new ArgumentException($"The {nameof(settings.WorkItemLockTimeout)} property value must be at least 10 seconds.", nameof(settings));
                }

                if (settings.WorkItemBatchSize < 10)
                {
                    throw new ArgumentException($"The {nameof(settings.WorkItemBatchSize)} property value must be at least 10.", nameof(settings));
                }
            }

            return settings;
        }

        async Task<SqlConnection> GetAndOpenConnectionAsync(CancellationToken cancelToken = default)
        {
            if (cancelToken == default)
            {
                cancelToken = this.ShutdownToken;
            }

            SqlConnection connection = this.settings.CreateConnection();
            await connection.OpenAsync(cancelToken);
            return connection;
        }

        SqlCommand GetTaskHubSprocCommand(SqlConnection connection, string sprocName)
        {
            SqlCommand command = connection.GetSprocCommand(string.Concat(this.settings.SchemaName, ".", sprocName));
            this.AddTaskHubNameParameter(command);
            return command;
        }

        SqlCommand GetTaskHubFuncCommand(SqlConnection connection, string funcName, SqlDbType retType, int retSize = 0)
        {
            SqlCommand command = connection.GetFuncCommand(string.Concat(this.settings.SchemaName, ".", funcName), retType, retSize);
            this.AddTaskHubNameParameter(command);
            return command;
        }

        void AddTaskHubNameParameter(SqlCommand command)
            => command.AddTaskHubNameParameter(this.taskHubNameValue);

        /// <summary>
        /// Cache value of task hub name parameter in the TaskHubMode=0
        /// </summary>
        /// <remarks>
        /// CurrentTaskHub T-SQL function compresses task hub value to fit column 
        /// of size 50. In TaskHubMode=0 one can avoid this cost by caching result 
        /// of the transformation. Caching makes more sense for worker, as it will 
        /// typically issue more calls. In TaskHubMode=1 parameter name is ignored. 
        /// </remarks>
        async Task CacheTaskHubNameValue()
        {
            if (this.taskHubNameValue.Length > 50)
            {
                using SqlConnection connection = await this.GetAndOpenConnectionAsync(this.ShutdownToken);
                using SqlCommand command = this.GetTaskHubFuncCommand(connection, "_CurrentTaskHub", SqlDbType.VarChar, 50);
                var taskHubName = await command.ExecuteFuncAsync<string>(this.ShutdownToken);
                // Logic in t-sql function will preserve first 16 characters.
                // Also, value is never null from executing db function
                if (0 == string.Compare(taskHubName, 0, this.taskHubNameValue, 0, 16, StringComparison.OrdinalIgnoreCase))
                    this.taskHubNameValue = taskHubName;
            }
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

        public override async Task StartAsync()
        {
            await base.StartAsync();
            await this.CacheTaskHubNameValue();
        }

        public override async Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            bool isWaiting = false;
            Stopwatch stopwatch = Stopwatch.StartNew();
            do
            {
                using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
                using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_LockNextOrchestration");

                int batchSize = this.settings.WorkItemBatchSize;
                DateTime lockExpiration = DateTime.UtcNow.Add(this.settings.WorkItemLockTimeout);

                command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
                command.Parameters.Add("@LockedBy", SqlDbType.VarChar, 100).Value = this.lockedByValue;
                command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

                DbDataReader reader;

                try
                {
                    reader = await SqlUtils.ExecuteReaderAsync(
                        command,
                        this.traceHelper,
                        instanceId: null,
                        cancellationToken);
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
                    var eventPayloadMappings = new EventPayloadMap(capacity: batchSize);

                    // Synchronous reads have significantly better performance: https://github.com/dotnet/SqlClient/issues/593
                    while (reader.Read())
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        TaskMessage message = reader.GetTaskMessage();
                        messages.Add(message);
                        Guid? payloadId = reader.GetPayloadId();
                        if (payloadId.HasValue)
                        {
                            // TODO: Need to understand what the payload behavior is for retry events
                            eventPayloadMappings.Add(message.Event, payloadId.Value);
                        }

                        // TODO: We're not currently using this value for anything. Ideally it would be included
                        //       in some logging that still needs to be introduced.
                        longestWaitTime = Math.Max(longestWaitTime, reader.GetInt32("WaitTime"));
                    }

                    if (messages.Count == 0)
                    {
                        if (!isWaiting)
                        {
                            this.traceHelper.GenericInfoEvent(
                                "No events were found. Waiting for new events to appear.",
                                instanceId: null);
                            isWaiting = true;
                        }

                        // TODO: Make this dynamic based on the number of readers
                        await this.orchestrationBackoffHelper.WaitAsync(cancellationToken);
                        continue;
                    }

                    this.orchestrationBackoffHelper.Reset();
                    isWaiting = false;

                    // Result #2: The runtime status of the orchestration instance
                    if (await reader.NextResultAsync(cancellationToken))
                    {
                        // Synchronous reads have significantly better performance: https://github.com/dotnet/SqlClient/issues/593
                        bool instanceExists = reader.Read();
                        string instanceId;
                        OrchestrationStatus? currentStatus;

                        bool isRunning = false;
                        if (instanceExists)
                        {
                            instanceId = SqlUtils.GetInstanceId(reader);
                            currentStatus = SqlUtils.GetRuntimeStatus(reader);
                            isRunning =
                                currentStatus == OrchestrationStatus.Running ||
                                currentStatus == OrchestrationStatus.Suspended ||
                                currentStatus == OrchestrationStatus.Pending;
                        }
                        else
                        {
                            instanceId = messages.Select(msg => msg.OrchestrationInstance.InstanceId).First();
                            currentStatus = null;
                        }

                        // If the instance is in a terminal state, log and discard the new events.
                        // NOTE: In the future, we may want to allow processing of some events if, for example, they may
                        //       change the state of a completed instance. For example, a rewind command.
                        if (!isRunning)
                        {
                            string warningMessage = instanceExists ?
                                $"Target is in the {currentStatus} state." :
                                $"Target doesn't exist (either never existed or continued-as-new).";

                            messages.ForEach(msg => this.traceHelper.DiscardingEvent(
                                msg.OrchestrationInstance.InstanceId,
                                msg.Event.EventType.ToString(),
                                DTUtils.GetTaskEventId(msg.Event),
                                warningMessage));

                            // Close the already opened reader so that we can execute a new command
                            reader.Close();

                            // Delete the events and release the orchestration instance lock
                            using SqlCommand discardCommand = this.GetTaskHubSprocCommand(connection, "_DiscardEventsAndUnlockInstance");
                            discardCommand.Parameters.Add("@InstanceID", SqlDbType.VarChar, 100).Value = instanceId;
                            discardCommand.Parameters.AddMessageIdParameter("@DeletedEvents", messages, this.settings.SchemaName);
                            try
                            {
                                await SqlUtils.ExecuteNonQueryAsync(
                                    discardCommand,
                                    this.traceHelper,
                                    instanceId,
                                    cancellationToken);
                            }
                            catch (Exception e)
                            {
                                this.traceHelper.ProcessingError(e, new OrchestrationInstance { InstanceId = instanceId });
                                throw;
                            }

                            continue;
                        }
                    }

                    // Result #3: The full event history for the locked instance
                    IList<HistoryEvent> history;
                    if (await reader.NextResultAsync(cancellationToken))
                    {
                        history = ReadHistoryEvents(reader, executionIdFilter: null, cancellationToken);
                    }
                    else
                    {
                        this.traceHelper.GenericWarning(
                            details: "Failed to read history from the database!",
                            instanceId: messages.FirstOrDefault(m => m.OrchestrationInstance?.InstanceId != null)?.OrchestrationInstance.InstanceId);
                        history = Array.Empty<HistoryEvent>();
                    }

                    var runtimeState = new OrchestrationRuntimeState(history);

                    string orchestrationName;
                    OrchestrationInstance instance;
                    if (runtimeState.ExecutionStartedEvent != null)
                    {
                        // This is an existing instance
                        orchestrationName = runtimeState.Name;
                        instance = runtimeState.OrchestrationInstance!;
                    }
                    else if (messages[0].Event is ExecutionStartedEvent startedEvent)
                    {
                        // This is a new manually-created instance
                        orchestrationName = startedEvent.Name;
                        instance = startedEvent.OrchestrationInstance;
                    }
                    else if (Entities.AutoStart(messages[0].OrchestrationInstance.InstanceId, messages) &&
                             messages[0].Event is ExecutionStartedEvent autoStartedEvent)
                    {
                        // This is a new auto-start instance (e.g. Durable Entities)
                        orchestrationName = autoStartedEvent.Name;
                        instance = autoStartedEvent.OrchestrationInstance;
                    }
                    else
                    {
                        // Don't know what to do with this message (TODO: Need to confirm behavior)
                        orchestrationName = "(Unknown)";
                        instance = new OrchestrationInstance();
                    }

                    return new ExtendedOrchestrationWorkItem(orchestrationName, instance, eventPayloadMappings)
                    {
                        InstanceId = messages[0].OrchestrationInstance.InstanceId,
                        LockedUntilUtc = lockExpiration,
                        NewMessages = messages,
                        OrchestrationRuntimeState = runtimeState,
                    };
                }
            } while (stopwatch.Elapsed < receiveTimeout);

            return null;
        }

        public override async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_RenewOrchestrationLocks");

            DateTime lockExpiration = DateTime.UtcNow.Add(this.settings.WorkItemLockTimeout);

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = workItem.InstanceId;
            command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, workItem.InstanceId);

            workItem.LockedUntilUtc = lockExpiration;
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

            this.traceHelper.CheckpointStarting(orchestrationState);
            Stopwatch sw = Stopwatch.StartNew();

            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_CheckpointOrchestration");

            OrchestrationInstance instance = newRuntimeState.OrchestrationInstance!;
            IList<HistoryEvent> newEvents = newRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = newRuntimeState.Events;
            int nextSequenceNumber = allEvents.Count - newEvents.Count;

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instance.InstanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = instance.ExecutionId;
            command.Parameters.Add("@RuntimeStatus", SqlDbType.VarChar, size: 30).Value = orchestrationState.OrchestrationStatus.ToString();
            command.Parameters.Add("@CustomStatusPayload", SqlDbType.VarChar).Value = orchestrationState.Status ?? SqlString.Null;

            currentWorkItem.EventPayloadMappings.Add(outboundMessages);
            currentWorkItem.EventPayloadMappings.Add(orchestratorMessages);

            command.Parameters.AddMessageIdParameter("@DeletedEvents", workItem.NewMessages, this.settings.SchemaName);

            command.Parameters.AddOrchestrationEventsParameter(
                "@NewOrchestrationEvents",
                orchestratorMessages,
                timerMessages,
                continuedAsNewMessage,
                currentWorkItem.EventPayloadMappings,
                this.settings.SchemaName);

            command.Parameters.AddTaskEventsParameter(
                "@NewTaskEvents",
                outboundMessages,
                currentWorkItem.EventPayloadMappings,
                this.settings.SchemaName);

            command.Parameters.AddHistoryEventsParameter(
                "@NewHistoryEvents",
                newEvents,
                instance,
                nextSequenceNumber,
                currentWorkItem.EventPayloadMappings,
                this.settings.SchemaName);

            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instance.InstanceId);
            }
            catch (SqlException e) when (SqlUtils.IsUniqueKeyViolation(e))
            {
                this.traceHelper.DuplicateExecutionDetected(instance, orchestrationState.Name);
                return;
            }

            // notify pollers that new messages may be available
            if (outboundMessages.Count > 0)
            {
                this.activityBackoffHelper.Reset();
            }

            if (orchestratorMessages.Count > 0 || timerMessages.Count > 0)
            {
                this.orchestrationBackoffHelper.Reset();
            }

            this.traceHelper.CheckpointCompleted(orchestrationState, sw);
        }

        // We abandon work items by just letting their locks expire. The benefit of this "lazy" approach is that it
        // removes the need for a DB access and also ensures that a work-item can't spam the error logs in a tight loop.
        public override Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem) => Task.CompletedTask;

        public override async Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken shutdownCancellationToken)
        {
            while (!shutdownCancellationToken.IsCancellationRequested)
            {
                using SqlConnection connection = await this.GetAndOpenConnectionAsync(shutdownCancellationToken);
                using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_LockNextTask");

                DateTime lockExpiration = DateTime.UtcNow.Add(this.settings.WorkItemLockTimeout);

                command.Parameters.Add("@LockedBy", SqlDbType.VarChar, size: 100).Value = this.lockedByValue;
                command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

                using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(
                    command,
                    this.traceHelper,
                    instanceId: null,
                    shutdownCancellationToken);
                if (!await reader.ReadAsync(shutdownCancellationToken))
                {
                    await this.activityBackoffHelper.WaitAsync(shutdownCancellationToken);
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

        public override async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_RenewTaskLocks");

            DateTime lockExpiration = DateTime.UtcNow.Add(this.settings.WorkItemLockTimeout);

            command.Parameters.AddMessageIdParameter("@RenewingTasks", workItem.TaskMessage, this.settings.SchemaName);
            command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = lockExpiration;

            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instanceId);

            workItem.LockedUntilUtc = lockExpiration;
            return workItem;
        }

        public override async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_CompleteTasks");

            command.Parameters.AddMessageIdParameter("@CompletedTasks", workItem.TaskMessage, this.settings.SchemaName);
            command.Parameters.AddTaskEventsParameter("@Results", responseMessage, this.settings.SchemaName);

            OrchestrationInstance instance = workItem.TaskMessage.OrchestrationInstance;
            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instance.InstanceId);
            }
            catch (SqlException e) when (e.Number == 50002)
            {
                // 50002 is the error code when we fail to delete the source message. This is expected to mean that
                // a task activity has executed twice, likely because of an unexpected lock expiration.
                string taskName = DTUtils.GetName(workItem.TaskMessage.Event) ?? "Unknown";
                this.traceHelper.DuplicateExecutionDetected(instance, taskName);
                return;
            }

            // signal the orchestration loop to poll immediately
            this.orchestrationBackoffHelper.Reset();
        }

        // We abandon work items by just letting their locks expire. The benefit of this "lazy" approach is that it
        // removes the need for a DB access and also ensures that a work-item can't spam the error logs in a tight loop.
        public override Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem) => Task.CompletedTask;

        public override async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "CreateInstance");

            ExecutionStartedEvent startEvent = (ExecutionStartedEvent)creationMessage.Event;
            OrchestrationInstance instance = startEvent.OrchestrationInstance;
            command.Parameters.Add("@Name", SqlDbType.VarChar, size: 300).Value = startEvent.Name;
            command.Parameters.Add("@Version", SqlDbType.VarChar, size: 100).Value = startEvent.Version;
            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instance.InstanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = instance.ExecutionId;
            command.Parameters.Add("@InputText", SqlDbType.VarChar).Value = startEvent.Input;
            command.Parameters.Add("@StartTime", SqlDbType.DateTime2).Value = startEvent.ScheduledStartTime;
            command.Parameters.Add("@TraceContext", SqlDbType.VarChar, size: 800).Value = SqlUtils.GetTraceContext(startEvent);

            if (dedupeStatuses?.Length > 0)
            {
                command.Parameters.Add("@DedupeStatuses", SqlDbType.VarChar).Value = string.Join(",", dedupeStatuses);
            }

            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instance.InstanceId);
            }
            catch (SqlException e) when (e.Number == 50001 /* Instance ID for pending/running instance already exists */)
            {
                // Try to avoid leaking SQL exception for issues like this
                throw new OrchestrationAlreadyExistsException(e.Message, e);
            }
        }

        public override async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_AddOrchestrationEvents");

            command.Parameters.AddOrchestrationEventsParameter("@NewOrchestrationEvents", message, this.settings.SchemaName);

            string instanceId = message.OrchestrationInstance.InstanceId;
            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instanceId);
        }

        public override async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            using var timeoutCts = timeout < TimeSpan.MaxValue && timeout >= TimeSpan.Zero ?
                new CancellationTokenSource(timeout) :
                new CancellationTokenSource();

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

                await Task.Delay(TimeSpan.FromSeconds(1), combinedCts.Token);
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
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "QuerySingleOrchestration");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@ExecutionID", SqlDbType.VarChar, size: 50).Value = executionId;

            using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(
                command,
                this.traceHelper,
                instanceId,
                cancellationToken);

            // Synchronous reads have significantly better performance: https://github.com/dotnet/SqlClient/issues/593
            if (reader.Read())
            {
                OrchestrationState state = reader.GetOrchestrationState();
                return state;
            }

            return null;
        }

        public override async Task<string> GetOrchestrationHistoryAsync(string instanceId, string? executionIdFilter)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "GetInstanceHistory");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@GetInputsAndOutputs", SqlDbType.Bit).Value = true; // There's no clean way for the caller to specify this currently

            using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper, instanceId);

            List<HistoryEvent> history = ReadHistoryEvents(reader, executionIdFilter);
            return DTUtils.SerializeToJson(history);
        }

        static List<HistoryEvent> ReadHistoryEvents(
            DbDataReader reader,
            string? executionIdFilter = null,
            CancellationToken cancellationToken = default)
        {
            var history = new List<HistoryEvent>(capacity: 128);

            // Synchronous reads have significantly better performance: https://github.com/dotnet/SqlClient/issues/593
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();

                string executionId = SqlUtils.GetExecutionId(reader)!;
                HistoryEvent e = reader.GetHistoryEvent(isOrchestrationHistory: true);
                if (executionIdFilter == null)
                {
                    executionIdFilter = executionId;
                }
                else if (executionIdFilter != executionId)
                {
                    // Either the instance with this execution ID doesn't exist or
                    // we're reading past the end of the current history (ContinueAsNew case).
                    break;
                }

                history.Add(e);
            }

            return history;
        }

        public override async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string? reason)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "TerminateInstance");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@Reason", SqlDbType.VarChar).Value = reason;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, instanceId);
        }

        public async Task<int> PurgeOrchestrationHistoryAsync(IEnumerable<string> instanceIds)
        {
            if (instanceIds?.Any() != true)
            {
                return 0;
            }

            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "PurgeInstanceStateByID");

            SqlParameter instancesDeletedReturnValue = command.Parameters.Add("@InstancesDeleted", SqlDbType.Int);
            instancesDeletedReturnValue.Direction = ParameterDirection.ReturnValue;

            command.Parameters.AddInstanceIDsParameter("@InstanceIDs", instanceIds, this.settings.SchemaName);

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
            int purgedInstanceCount = (int)instancesDeletedReturnValue.Value;
            if (purgedInstanceCount > 0)
            {
                this.traceHelper.PurgedInstances(
                    this.userId,
                    purgedInstanceCount,
                    latencyStopwatch);
            }

            return purgedInstanceCount;
        }

        public override async Task PurgeOrchestrationHistoryAsync(
            DateTime maxThresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "PurgeInstanceStateByTime");

            command.Parameters.Add("@ThresholdTime", SqlDbType.DateTime2).Value = maxThresholdDateTimeUtc;
            command.Parameters.Add("@FilterType", SqlDbType.TinyInt).Value = (int)timeRangeFilterType;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
        }

        public override async Task<PurgeResult> PurgeInstanceStateAsync(string instanceId)
        {
            int purgedInstanceCount = await this.PurgeOrchestrationHistoryAsync(new[] { instanceId });
            return new PurgeResult(purgedInstanceCount);
        }

        public override async Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
        {
            var purgeQuery = new SqlOrchestrationQuery
            {
                PageSize = 1000,
                CreatedTimeFrom = purgeInstanceFilter.CreatedTimeFrom,
                FetchInput = false,
                FetchOutput = false,
            };
            
            if (purgeInstanceFilter.CreatedTimeTo != null)
            {
                purgeQuery.CreatedTimeTo = purgeInstanceFilter.CreatedTimeTo.Value;
            }

            if (purgeInstanceFilter.RuntimeStatus?.Any() == true)
            {
                purgeQuery.StatusFilter = new HashSet<OrchestrationStatus>(purgeInstanceFilter.RuntimeStatus);
            }
            
            IReadOnlyCollection<OrchestrationState> results = await this.GetManyOrchestrationsAsync(purgeQuery, CancellationToken.None);
                                                                                                  
            IEnumerable<string> instanceIds = results.Select(r => r.OrchestrationInstance.InstanceId);
            int purgedInstanceCount = await this.PurgeOrchestrationHistoryAsync(instanceIds);
            return new PurgeResult(purgedInstanceCount);
        }

        public override async Task<OrchestrationQueryResult> GetOrchestrationWithQueryAsync(OrchestrationQuery query, CancellationToken cancellationToken)
        {
            if (query.TaskHubNames?.Any() == true)
            {
                throw new NotSupportedException("Querying orchestrations by task hub name is not supported.");
            }

            var sqlOrchestrationQuery = new SqlOrchestrationQuery
            {
                CreatedTimeFrom = query.CreatedTimeFrom.GetValueOrDefault(),
                CreatedTimeTo = query.CreatedTimeTo.GetValueOrDefault(),
                FetchInput = query.FetchInputsAndOutputs,
                FetchOutput = query.FetchInputsAndOutputs,
                InstanceIdPrefix = query.InstanceIdPrefix,
                PageSize = query.PageSize,
            };

            if (query.RuntimeStatus?.Any() == true)
            {
                sqlOrchestrationQuery.StatusFilter = new HashSet<OrchestrationStatus>(query.RuntimeStatus);
            }

            // The continuation token is just a page number.
            if (string.IsNullOrWhiteSpace(query.ContinuationToken))
            {
                sqlOrchestrationQuery.PageNumber = 0;
            }
            else if (int.TryParse(query.ContinuationToken, out int pageNumber))
            {
                sqlOrchestrationQuery.PageNumber = pageNumber;
            }
            else
            {
                throw new ArgumentException($"The continuation token '{query.ContinuationToken}' is invalid.", nameof(query));
            }

            IReadOnlyCollection<OrchestrationState> results = await this.GetManyOrchestrationsAsync(sqlOrchestrationQuery, cancellationToken);
            string? continuationToken = 
                results.Count == sqlOrchestrationQuery.PageSize ? (sqlOrchestrationQuery.PageNumber + 1).ToString() : null;
            return new OrchestrationQueryResult(results, continuationToken);
        }

        /// <summary>
        /// Queries the database for all orchestration instances that match a given filter.
        /// </summary>
        /// <param name="query">The query parameters to use as a filter.</param>
        /// <param name="cancellationToken">A token for cancelling the query.</param>
        /// <returns>Returns a collection of <see cref="OrchestrationState"/> objects for orchestrations that match the specified query filter.</returns>
        public async Task<IReadOnlyCollection<OrchestrationState>> GetManyOrchestrationsAsync(SqlOrchestrationQuery query, CancellationToken cancellationToken)
        {
            if (query.PageSize < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(query), $"{nameof(query.PageSize)} must be a positive number.");
            }

            if (query.PageNumber < 0 || query.PageNumber > short.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(query), $"{nameof(query.PageNumber)} must be between 0 and {short.MaxValue} (inclusive).");
            }

            DateTime createdTimeFrom = query.CreatedTimeFrom.ToSqlUtcDateTime(DateTime.MinValue);
            DateTime createdTimeTo = query.CreatedTimeTo.ToSqlUtcDateTime(DateTime.MaxValue);

            using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_QueryManyOrchestrations");

            command.Parameters.Add("@PageSize", SqlDbType.SmallInt).Value = query.PageSize;
            command.Parameters.Add("@PageNumber", SqlDbType.Int).Value = query.PageNumber;
            command.Parameters.Add("@FetchInput", SqlDbType.Bit).Value = query.FetchInput;
            command.Parameters.Add("@FetchOutput", SqlDbType.Bit).Value = query.FetchOutput;
            command.Parameters.Add("@CreatedTimeFrom", SqlDbType.DateTime2).Value = createdTimeFrom;
            command.Parameters.Add("@CreatedTimeTo", SqlDbType.DateTime2).Value = createdTimeTo;
            command.Parameters.Add("@InstanceIDPrefix", SqlDbType.VarChar, size: 100).Value = query.InstanceIdPrefix ?? SqlString.Null;
            command.Parameters.Add("@ExcludeSubOrchestrations", SqlDbType.Bit).Value = query.ExcludeSubOrchestrations;

            if (query.StatusFilter?.Count > 0)
            {
                string filter = string.Join(",", query.StatusFilter);
                command.Parameters.Add("@RuntimeStatusFilter", SqlDbType.VarChar, size: 200).Value = filter;
            }

            using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(
                command,
                this.traceHelper,
                instanceId: null,
                cancellationToken);

            var results = new List<OrchestrationState>(query.PageSize);
            while (!cancellationToken.IsCancellationRequested && reader.Read())
            {
                OrchestrationState state = reader.GetOrchestrationState();
                results.Add(state);
            }

            return results;
        }

        /// <summary>
        /// Rewinds a failed orchestration to the point right before it failed.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration to rewind.</param>
        /// <param name="reason">A reason for rewinding the orchestration.</param>
        public async Task RewindTaskOrchestrationAsync(string instanceId, string reason)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync();
            using SqlCommand command = this.GetTaskHubSprocCommand(connection, "_RewindInstance");

            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, size: 100).Value = instanceId;
            command.Parameters.Add("@Reason", SqlDbType.VarChar).Value = reason;

            await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
        }

        /// <summary>
        /// Gets a value that represents the recommended instance count for handling the current event and work-item backlogs.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method calls the <c>dt._GetScaleRecommendation</c> SQL function and gets back a number that represents
        /// the recommended number of worker replicas that should be allocated to handle the current event backlog.
        /// The SQL function takes the values of <see cref="SqlOrchestrationServiceSettings.MaxConcurrentActivities"/> and
        /// <see cref="SqlOrchestrationServiceSettings.MaxActiveOrchestrations"/> as inputs.
        /// </para><para>
        /// This method is intended to be called periodically by auto-scale providers for making periodic scale decisions.
        /// The returned value may be zero, in which case it is assumed to be safe to scale the number of task hub workers
        /// down to zero.
        /// </para>
        /// </remarks>
        /// <param name="currentReplicaCount">
        /// If specified, results in a trace message if the recommended replica count differs from this value.
        /// </param>
        /// <returns>Returns a non-negative recommended replica count integer value.</returns>
        public async Task<int> GetRecommendedReplicaCountAsync(int? currentReplicaCount = null, CancellationToken cancellationToken = default)
        {
            using SqlConnection connection = await this.GetAndOpenConnectionAsync(cancellationToken);
            using SqlCommand command = this.GetTaskHubFuncCommand(connection, "_GetScaleRecommendation", SqlDbType.Int);
            command.Parameters.Add("@MaxOrchestrationsPerWorker", SqlDbType.Int).Value = this.MaxConcurrentTaskOrchestrationWorkItems;
            command.Parameters.Add("@MaxActivitiesPerWorker", SqlDbType.Int).Value = this.MaxConcurrentTaskActivityWorkItems;
            int recommendedReplicaCount = await command.ExecuteFuncAsync<int>(cancellationToken);
            if (currentReplicaCount != null && currentReplicaCount != recommendedReplicaCount)
            {
                this.traceHelper.ReplicaCountChangeRecommended(currentReplicaCount.Value, recommendedReplicaCount);
            }

            return recommendedReplicaCount;
        }

        class ExtendedOrchestrationWorkItem : TaskOrchestrationWorkItem
        {
            public ExtendedOrchestrationWorkItem(
                string name,
                OrchestrationInstance instance,
                EventPayloadMap eventPayloadMap)
            {
                this.Name = name;
                this.Instance = instance;
                this.EventPayloadMappings = eventPayloadMap;
            }

            public string Name { get; }

            public OrchestrationInstance Instance { get; }

            public EventPayloadMap EventPayloadMappings { get; }
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
