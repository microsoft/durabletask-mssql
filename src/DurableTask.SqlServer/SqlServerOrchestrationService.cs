namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.SqlServer.Utils;

    public class SqlServerOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient
    {
        readonly BackoffPollingHelper backoffHelper = new BackoffPollingHelper(
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(3)); // TODO: Configurable
        
        readonly AsyncQueue<DbTaskEvent> activityQueue = new AsyncQueue<DbTaskEvent>();

        readonly SqlServerProviderOptions options;
        readonly TraceHelper traceHelper;

        int inFlightActivities = 0;

        public SqlServerOrchestrationService(SqlServerProviderOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.traceHelper = new TraceHelper(this.options.LoggerFactory.CreateLogger("DurableTask.SqlServer"));
        }

        public int MaxOrchestrationConcurrency => this.options.MaxOrchestrationConcurrency;

        public int MaxActivityConcurrency => this.options.MaxActivityConcurrency;

        public DbConnection GetConnection() => new SqlConnection(this.options.ConnectionString);

        int IOrchestrationService.TaskOrchestrationDispatcherCount => Environment.ProcessorCount;

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => this.MaxOrchestrationConcurrency;

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        int IOrchestrationService.TaskActivityDispatcherCount => Environment.ProcessorCount;

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => this.MaxActivityConcurrency;

        // TODO: https://stackoverflow.com/questions/650098/how-to-execute-an-sql-script-file-using-c-sharp
        Task IOrchestrationService.CreateAsync() => throw new NotImplementedException();

        Task IOrchestrationService.CreateAsync(bool recreateInstanceStore) => throw new NotImplementedException();

        Task IOrchestrationService.CreateIfNotExistsAsync() => throw new NotImplementedException();

        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            throw new NotImplementedException();
        }

        Task IOrchestrationService.DeleteAsync()
        {
            throw new NotImplementedException();
        }

        Task IOrchestrationService.DeleteAsync(bool deleteInstanceStore)
        {
            throw new NotImplementedException();
        }

        Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 10; // TODO: Configurable
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 10; // TODO: Configurable
        }

        Task<string> IOrchestrationServiceClient.GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        Task<IList<OrchestrationState>> IOrchestrationServiceClient.GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            throw new NotImplementedException();
        }

        Task<OrchestrationState> IOrchestrationServiceClient.GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        bool IOrchestrationService.IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        async Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(
            TaskMessage creationMessage,
            OrchestrationStatus[] dedupeStatuses)
        {
            this.traceHelper.SchedulingOrchestration((ExecutionStartedEvent)creationMessage.Event);

            using DbConnection connection = this.GetConnection();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // TODO: Configurable
            await connection.OpenAsync(timeout.Token);

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "CreateInstances";

            // TODO: support for dedupeStatuses
            SqlParameter parameter = command.Parameters.Add("@NewInstanceEvents", SqlDbType.Structured);
            parameter.TypeName = "TaskEvents";
            parameter.Value = creationMessage.ToTableValueParameter();

            Stopwatch stopwatch = Stopwatch.StartNew();

            try
            {
                await command.ExecuteNonQueryAsync(timeout.Token);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, creationMessage.OrchestrationInstance);
                throw;
            }
            finally
            {
                this.traceHelper.SprocCompleted(command.CommandText, stopwatch);
            }
        }

        async Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);

            while (true)
            {
                using (DbConnection connection = this.GetConnection())
                using (SqlCommand command = (SqlCommand)connection.CreateCommand())
                {
                    await connection.OpenAsync(combinedCts.Token);

                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "QuerySingleOrchestration";

                    command.Parameters.AddWithValue("@InstanceID", instanceId);
                    command.Parameters.AddWithValue("@ExecutionID", executionId);

                    SqlDataReader reader;

                    Stopwatch latencyStopwatch = Stopwatch.StartNew();
                    try
                    {
                        reader = await command.ExecuteReaderAsync(combinedCts.Token);
                    }
                    catch (Exception e)
                    {
                        this.traceHelper.ProcessingError(e, new OrchestrationInstance { InstanceId = instanceId, ExecutionId = executionId });
                        throw;
                    }
                    finally
                    {
                        this.traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
                    }

                    using (reader)
                    {
                        if (await reader.ReadAsync(combinedCts.Token))
                        {
                            OrchestrationState state = reader.GetOrchestrationState();
                            if (state.OrchestrationStatus == OrchestrationStatus.Completed ||
                                state.OrchestrationStatus == OrchestrationStatus.Failed ||
                                state.OrchestrationStatus == OrchestrationStatus.Terminated)
                            {
                                return state;
                            }
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(1), combinedCts.Token);
            }
        }

        async Task<TaskOrchestrationWorkItem?> IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(cancellationToken);

            int batchSize = 10; // TODO: Configurable
            DateTime lockExpiration = DateTime.UtcNow.AddMinutes(2); // TODO: Configurable
            string lockedBy = Environment.MachineName; //  TODO: Configurable

            using DbCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "LockNextOrchestration";
            command.AddParameter("@BatchSize", batchSize); 
            command.AddParameter("@LockedBy", lockedBy);
            command.AddParameter("@LockExpiration", lockExpiration);

            DbDataReader reader;

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                reader = await command.ExecuteReaderAsync(cancellationToken);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, new OrchestrationInstance());
                throw;
            }
            finally
            {
                this.traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
            }

            using (reader)
            {
                // Result #1: The list of control queue messages
                int longestWaitTime = 0;
                var messages = new List<TaskMessage>(batchSize);
                while (await reader.ReadAsync(cancellationToken))
                {
                    messages.Add(reader.GetTaskMessage());
                    longestWaitTime = Math.Max(longestWaitTime, reader.GetInt32("WaitTime"));
                }

                if (messages.Count == 0)
                {
                    await this.backoffHelper.WaitAsync(cancellationToken);
                    return null;
                }

                this.backoffHelper.Reset();

                // Result #2: The full event history for the locked instance
                var history = new List<HistoryEvent>();
                if (await reader.NextResultAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        history.Add(reader.GetHistoryEvent(isOrchestrationHistory: true));
                    }
                }

                var runtimeState = new OrchestrationRuntimeState(history);

                string orchestrationName;
                OrchestrationInstance instance;
                if (runtimeState.ExecutionStartedEvent != null)
                {
                    orchestrationName = runtimeState.Name;
                    instance = runtimeState.OrchestrationInstance;
                }
                else if (messages[0].Event is ExecutionStartedEvent startedEvent)
                {
                    orchestrationName = startedEvent.Name;
                    instance = startedEvent.OrchestrationInstance;
                }
                else
                {
                    orchestrationName = "(Unknown)";
                    instance = new OrchestrationInstance();
                }
                
                this.traceHelper.ResumingOrchestration(orchestrationName, instance, longestWaitTime);

                return new TaskOrchestrationWorkItem
                {
                    InstanceId = messages[0].OrchestrationInstance.InstanceId,
                    LockedUntilUtc = lockExpiration,
                    NewMessages = messages,
                    OrchestrationRuntimeState = runtimeState,
                };
            }
        }

        Task IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        async Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            this.traceHelper.CheckpointingOrchestration(orchestrationState);

            var dbTaskEvents = new List<DbTaskEvent>(capacity: Math.Min(outboundMessages.Count, this.options.MaxActivityConcurrency - this.inFlightActivities));
            foreach (TaskMessage message in outboundMessages)
            {
                string? lockedBy = null;
                DateTime? lockExpiration = null;
                bool isLocal = false;

                if (this.inFlightActivities <= this.options.MaxActivityConcurrency)
                {
                    if (Interlocked.Increment(ref this.inFlightActivities) <= this.options.MaxActivityConcurrency)
                    {
                        lockedBy = this.options.AppName;
                        lockExpiration = DateTime.UtcNow.Add(this.options.TaskEventLockTimeout);
                        isLocal = true;
                    }
                    else
                    {
                        // Undo the previous increment
                        Interlocked.Decrement(ref this.inFlightActivities);
                    }
                }

                TaskScheduledEvent taskEvent = (TaskScheduledEvent)message.Event;
                dbTaskEvents.Add(new DbTaskEvent(message, taskEvent.EventId, lockedBy, lockExpiration));
             
                this.traceHelper.SchedulingActivity(taskEvent, message.OrchestrationInstance, isLocal);
            }

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // TODO: Configurable
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(timeout.Token);

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "CheckpointOrchestration";

            OrchestrationInstance instance = newRuntimeState.OrchestrationInstance;
            IList<HistoryEvent> newEvents = newRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = newRuntimeState.Events;
            int nextSequenceNumber = allEvents.Count - newEvents.Count;

            var combinedOrchestratorMessages = new List<TaskMessage>(orchestratorMessages.Count + timerMessages.Count + 1);
            combinedOrchestratorMessages.AddRange(orchestratorMessages);
            combinedOrchestratorMessages.AddRange(timerMessages);
            if (continuedAsNewMessage != null)
            {
                combinedOrchestratorMessages.Add(continuedAsNewMessage);
            }

            SqlParameter p1 = command.Parameters.Add("@NewOrchestrationEvents", SqlDbType.Structured);
            p1.TypeName = "TaskEvents";
            p1.Value = combinedOrchestratorMessages.ToTableValueParameter();

            SqlParameter p2 = command.Parameters.Add("@NewHistoryEvents", SqlDbType.Structured);
            p2.TypeName = "TaskEvents";
            p2.Value = newEvents.ToTableValueParameter(instance, nextSequenceNumber);

            SqlParameter p3 = command.Parameters.Add("@NewTaskEvents", SqlDbType.Structured);
            p3.TypeName = "TaskEvents";
            p3.Value = dbTaskEvents.ToTableValueParameter();

            SqlParameter p4 = command.Parameters.Add("@UpdatedInstanceStatus", SqlDbType.Structured);
            p4.TypeName = "TaskEvents";
            p4.Value = orchestrationState.ToTableValueParameter(lockExpiration: null);

            SqlParameter p5 = command.Parameters.Add("@DeletedControlMessages", SqlDbType.Structured);
            p5.TypeName = "TaskEvents";
            p5.Value = workItem.NewMessages.ToTableValueParameter(onlyMessageId: true);

            SqlDataReader reader;

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                reader = await command.ExecuteReaderAsync(timeout.Token);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, instance);
                throw;
            }
            finally
            {
                this.traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
            }

            // Update the sequence number values for all the locally added task messages
            IDictionary<int, DbTaskEvent> localEvents = dbTaskEvents.Where(e => e.IsLocal).ToDictionary(e => e.TaskId);
            using (reader)
            {
                // TODO: Read the results of the first result set to learn if any deleted events were not found.
                if (await reader.NextResultAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        int taskId = SqlUtils.GetTaskId(reader);
                        if (localEvents.TryGetValue(taskId, out DbTaskEvent localEvent))
                        {
                            long sequenceNumber = SqlUtils.GetSequenceNumber(reader);
                            localEvent.Message.SequenceNumber = sequenceNumber;
                        }
                    }
                }
            }

            // Local events can be dispatched directly using the local activity queue.
            foreach (DbTaskEvent taskEvent in localEvents.Values)
            {
                this.activityQueue.Enqueue(taskEvent);
            }

            this.backoffHelper.Reset();
        }

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.CompletedTask;
        }

        async Task<TaskActivityWorkItem?> IOrchestrationService.LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken shutdownToken)
        {
            if (this.inFlightActivities > this.options.MaxActivityConcurrency)
            {
                // In theory the core framework shouldn't ever call this because it keeps track of concurrency
                // settings itself. However, we have optimizations in place that can cause our value to differ
                // slightly for short periods of time.
                await Task.Delay(TimeSpan.FromSeconds(1));
                return null;
            }

            // Slow path (polling)
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(shutdownToken);

            DateTime lockExpiration = DateTime.UtcNow.Add(this.options.TaskEventLockTimeout);
            string lockedBy = this.options.AppName;

            using DbCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "LockNextTask";
            command.AddParameter("@LockedBy", lockedBy);
            command.AddParameter("@LockExpiration", lockExpiration);

            DbDataReader reader;

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                reader = await command.ExecuteReaderAsync(shutdownToken);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, new OrchestrationInstance());
                throw;
            }
            finally
            {
                this.traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
            }

            if (await reader.ReadAsync(shutdownToken))
            {
                TaskMessage taskMessage = reader.GetTaskMessage();
                int waitTime = reader.GetInt32("WaitTime");

                this.traceHelper.StartingActivity((TaskScheduledEvent)taskMessage.Event, taskMessage.OrchestrationInstance, waitTime);

                Interlocked.Increment(ref this.inFlightActivities);

                return new TaskActivityWorkItem
                {
                    Id = $"{taskMessage.OrchestrationInstance.InstanceId}::{taskMessage.SequenceNumber:X8}",
                    TaskMessage = taskMessage,
                    LockedUntilUtc = lockExpiration,
                };
            }

            // Didn't find any read activity task events in the DB, so wait for notifications.
            // TODO: Need to revisit timeout strategy, and make sure it's not possible for us to block while
            //       work items are running in the DB.
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, shutdownToken);

            try
            {
                DbTaskEvent taskEvent = await this.activityQueue.DequeueAsync(combinedCts.Token);
                this.traceHelper.StartingActivity((TaskScheduledEvent)taskEvent.Message.Event, taskEvent.Message.OrchestrationInstance, 0);
                return new TaskActivityWorkItem
                {
                    Id = $"{taskEvent.Message.OrchestrationInstance.InstanceId}::{taskEvent.Message.SequenceNumber:X8}",
                    TaskMessage = taskEvent.Message,
                    // TODO: Assert that taskEvent.LockExpiration is never null
                    LockedUntilUtc = taskEvent.LockExpiration ?? DateTime.UtcNow.Add(this.options.TaskEventLockTimeout),
                };
            }
            catch (OperationCanceledException)
            {
                // ignore
            }

            // timed-out waiting for work.
            return null;
        }

        Task IOrchestrationService.AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            Interlocked.Decrement(ref this.inFlightActivities);
            throw new NotImplementedException();
        }

        async Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem,
            TaskMessage responseMessage)
        {
            DTUtils.GetTaskCompletionStatus(responseMessage, out int taskEventId, out bool succeeded);
            this.traceHelper.CompletingActivity(workItem.TaskMessage.OrchestrationInstance, taskEventId, succeeded);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // TODO: Configurable
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(timeout.Token);

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "CompleteTasks";

            SqlParameter p1 = command.Parameters.Add("@CompletedTasks", SqlDbType.Structured);
            p1.TypeName = "TaskEvents";
            p1.Value = workItem.TaskMessage.ToTableValueParameter(onlyMessageId: true);

            SqlParameter p2 = command.Parameters.Add("@Results", SqlDbType.Structured);
            p2.TypeName = "TaskEvents";
            p2.Value = responseMessage.ToTableValueParameter();

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                await command.ExecuteNonQueryAsync(timeout.Token);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, workItem.TaskMessage.OrchestrationInstance);
                throw;
            }
            finally
            {
                this.traceHelper.SprocCompleted(command.CommandText, latencyStopwatch);
            }

            Interlocked.Decrement(ref this.inFlightActivities);
            this.backoffHelper.Reset();
        }

        Task IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            return Task.FromResult(workItem); // TODO
        }

        Task IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.CompletedTask; // TODO
        }

        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            throw new NotImplementedException();
        }

        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            if (!messages.Any())
            {
                return Task.CompletedTask;
            }

            throw new NotImplementedException();
        }

        Task IOrchestrationService.StartAsync() => Task.CompletedTask; // TODO

        Task IOrchestrationService.StopAsync() => Task.CompletedTask; // TODO

        Task IOrchestrationService.StopAsync(bool isForced) => Task.CompletedTask; // TODO
    }
}
