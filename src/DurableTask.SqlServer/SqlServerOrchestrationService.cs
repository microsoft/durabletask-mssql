namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.SqlServer.Utils;
    using Microsoft.Data.SqlClient;
    using Microsoft.SqlServer.Management.Common;
    using SemVersion;

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

        public SqlConnection GetConnection() => this.options.CreateConnection();

        int IOrchestrationService.TaskOrchestrationDispatcherCount => Environment.ProcessorCount;

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => this.MaxOrchestrationConcurrency;

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        int IOrchestrationService.TaskActivityDispatcherCount => Environment.ProcessorCount;

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => this.MaxActivityConcurrency;

        Task IOrchestrationService.CreateIfNotExistsAsync() => ((IOrchestrationService)this).CreateAsync(recreateInstanceStore: false);

        // TODO: This should fail if the schema already exists.
        Task IOrchestrationService.CreateAsync() => ((IOrchestrationService)this).CreateAsync(recreateInstanceStore: false);

        async Task IOrchestrationService.CreateAsync(bool recreateInstanceStore)
        {
            // Prevent other create or delete operations from executing at the same time.
            await using DatabaseLock dbLock = await this.AcquireDatabaseLockAsync();

            var currentSchemaVersion = new SemanticVersion(0, 0, 0);
            if (recreateInstanceStore)
            {
                await this.DropSchemaAsync(dbLock);
            }
            else
            {
                // If the database already has the latest schema, then skip
                using SqlCommand command = dbLock.CreateCommand();
                command.CommandText = "dt.GetVersions";
                command.CommandType = CommandType.StoredProcedure;

                try
                {
                    using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper);
                    if (await reader.ReadAsync())
                    {
                        // The first result contains the latest version
                        currentSchemaVersion = SqlUtils.GetVersion(reader);
                        if (currentSchemaVersion >= DTUtils.ExtensionVersion)
                        {
                            // The schema is already up-to-date.
                            return;
                        }
                    }
                }
                catch (SqlException e) when (e.Number == 2812 /* Could not find stored procedure */)
                {
                    // Ignore - this is expected for new databases
                }
            }

            // SQL schema setup scripts are embedded resources in the assembly, making them immutable post-build.
            Assembly assembly = typeof(SqlServerOrchestrationService).Assembly;
            IEnumerable<string> createSchemaFiles = assembly.GetManifestResourceNames()
                .Where(name => name.Contains(".schema-") && name.EndsWith(".sql"));

            var versionedFiles = new Dictionary<SemanticVersion, string>();
            foreach (string name in createSchemaFiles)
            {
                // Attempt to parse the semver-like string from the resource name.
                // This version number tells us whether to execute the script for this extension version.
                const string RegexExpression = @"schema-(\d+.\d+.\d+(?:-\w+)?).sql$";
                Match match = Regex.Match(name, RegexExpression);
                if (!match.Success || match.Groups.Count < 2)
                {
                    throw new InvalidOperationException($"Failed to find version information in resource name '{name}'. The resource name must match the regex expression '{RegexExpression}'.");
                }

                SemanticVersion version = SemanticVersion.Parse(match.Groups[1].Value);
                if (!versionedFiles.TryAdd(version, match.Value))
                {
                    throw new InvalidOperationException($"There must not be more than one script resource with the same version number! Found {version} multiple times.");
                }
            }

            // Sort by the version numbers to ensure that we run them in the correct order
            foreach ((SemanticVersion version, string name) in versionedFiles.OrderBy(pair => pair.Key))
            {
                // Skip past versions that are already present in the database
                if (version > currentSchemaVersion)
                {
                    await this.ExecuteSqlScriptAsync(name, dbLock);
                    currentSchemaVersion = version;
                }
            }

            // Add or update stored procedures
            await this.ExecuteSqlScriptAsync("sprocs.sql", dbLock);

            // TODO: Add or update views in views.sql

            // Insert the current extension version number into the database and commit the transaction.
            // The extension version is used instead of the schema version to more accurately track whether
            // we need to update the sprocs or views.
            using (SqlCommand command = dbLock.CreateCommand())
            {
                command.CommandText = "dt.UpdateVersion";
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add("@SemanticVersion", SqlDbType.NVarChar, 100).Value = DTUtils.ExtensionVersion.ToString();
                
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
            }

            await dbLock.CommitAsync();
        }

        Task IOrchestrationService.DeleteAsync() => ((IOrchestrationService)this).DeleteAsync(deleteInstanceStore: true);

        async Task IOrchestrationService.DeleteAsync(bool deleteInstanceStore)
        {
            if (!deleteInstanceStore)
            {
                throw new NotSupportedException($"Setting the {deleteInstanceStore} parameter to false is not supported.");
            }

            // Prevent other create or delete operations from executing at the same time.
            await using DatabaseLock dbLock = await this.AcquireDatabaseLockAsync();
            await this.DropSchemaAsync(dbLock);
            await dbLock.CommitAsync();
        }

        Task DropSchemaAsync(DatabaseLock dbLock) => this.ExecuteSqlScriptAsync("drop-schema.sql", dbLock);

        async Task<DatabaseLock> AcquireDatabaseLockAsync()
        {
            SqlConnection connection = this.GetConnection();
            await connection.OpenAsync();

            // It's possible that more than one worker may attempt to execute this creation logic at the same
            // time. To avoid update conflicts, we use an app lock + a transaction to ensure only a single worker
            // can perform an upgrade at a time. All other workers will wait for the first one to complete.
            const string LockName = "DURABLE_TASK_SCHEMA_UPGRADE_LOCK";

            SqlTransaction lockTransaction = (SqlTransaction)await connection.BeginTransactionAsync();

            using (SqlCommand command = connection.CreateCommand())
            {
                // Reference https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql#syntax
                command.CommandText = "sys.sp_getapplock";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = lockTransaction;
                command.Parameters.Add("@Resource", SqlDbType.NVarChar, 256).Value = LockName;
                command.Parameters.Add("@LockMode", SqlDbType.VarChar, 32).Value = "Update";
                command.Parameters.Add("@LockOwner", SqlDbType.VarChar, 32).Value = "Transaction";
                command.Parameters.Add("@RETURN_VALUE", SqlDbType.SmallInt).Direction = ParameterDirection.ReturnValue;

                // This command will "block" if the app lock is held by another process or thread.
                Stopwatch latencyStopwatch = Stopwatch.StartNew();
                await command.ExecuteNonQueryAsync();
                latencyStopwatch.Stop();

                int returnCode = (int)command.Parameters["@RETURN_VALUE"].Value;
                if (returnCode < 0)
                {
                    throw new InvalidOperationException($"Failed to acquire a lock on resource '{LockName}'. Return code: {returnCode}.");
                }

                this.traceHelper.AcquiredAppLock(returnCode, latencyStopwatch);
            }

            return new DatabaseLock(connection, lockTransaction);
        }

        async Task ExecuteSqlScriptAsync(string scriptName, DatabaseLock dbLock)
        {
            // We don't actually use the lock here, but want to make sure the caller is holding it.
            if (dbLock == null)
            {
                throw new ArgumentNullException(nameof(dbLock));
            }

            if (!dbLock.IsHeld)
            {
                throw new ArgumentException("This database lock has already been released!", nameof(dbLock));
            }

            string schemaCommands = await GetScriptTextAsync(scriptName);

            // Reference: https://stackoverflow.com/questions/650098/how-to-execute-an-sql-script-file-using-c-sharp
            await using SqlConnection scriptRunnerConnection = this.GetConnection();
            var serverConnection = new ServerConnection(scriptRunnerConnection);

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                // NOTE: Async execution is not supported by this library
                serverConnection.ExecuteNonQuery(schemaCommands);
            }
            finally
            {
                latencyStopwatch.Stop();
                this.traceHelper.ExecutedSqlScript(scriptName, latencyStopwatch);
            }
        }

        static Task<string> GetScriptTextAsync(string scriptName, Assembly? assembly = null)
        {
            if (assembly == null)
            {
                assembly = typeof(SqlServerOrchestrationService).Assembly;
            }

            string assemblyName = assembly.GetName().Name;
            if (!scriptName.StartsWith(assemblyName))
            {
                scriptName = $"{assembly.GetName().Name}.Scripts.{scriptName}";
            }

            using Stream resourceStream = assembly.GetManifestResourceStream(scriptName);
            if (resourceStream == null)
            {
                throw new ArgumentException($"Could not find assembly resource named '{scriptName}'.");
            }

            using var reader = new StreamReader(resourceStream);
            return reader.ReadToEndAsync();
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

        async Task<IList<OrchestrationState>> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId,
            bool allExecutions)
        {
            OrchestrationState? state = await this.GetOrchestrationStateAsync(
                instanceId,
                null /* executionId */,
                CancellationToken.None);
            if (state == null)
            {
                return Array.Empty<OrchestrationState>();
            }

            return new[] { state };
        }

        Task<OrchestrationState?> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId,
            string executionId)
        {
            return this.GetOrchestrationStateAsync(
                instanceId,
                executionId,
                CancellationToken.None);
        }

        async Task<OrchestrationState?> GetOrchestrationStateAsync(
            string instanceId,
            string? executionId,
            CancellationToken cancellationToken)
        {
            using DbConnection connection = this.GetConnection();
            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            await connection.OpenAsync(cancellationToken);

            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "dt.QuerySingleOrchestration";

            command.Parameters.AddWithValue("@InstanceID", instanceId);
            command.Parameters.AddWithValue("@ExecutionID", executionId);

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

        bool IOrchestrationService.IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState) => false;

        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage) =>
            ((IOrchestrationServiceClient)this).CreateTaskOrchestrationAsync(creationMessage, Array.Empty<OrchestrationStatus>());

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
            command.CommandText = "dt.CreateInstances";

            // TODO: support for dedupeStatuses
            SqlParameter parameter = command.Parameters.Add("@NewInstanceEvents", SqlDbType.Structured);
            parameter.TypeName = " dt.TaskEvents";
            parameter.Value = creationMessage.ToTableValueParameter();

            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, timeout.Token);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, creationMessage.OrchestrationInstance);
                throw;
            }
        }

        async Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
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
            command.CommandText = "dt.LockNextOrchestration";
            command.AddParameter("@BatchSize", batchSize); 
            command.AddParameter("@LockedBy", lockedBy);
            command.AddParameter("@LockExpiration", lockExpiration);

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
            return Task.CompletedTask;
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
            command.CommandText = "dt.CheckpointOrchestration";

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
            p1.TypeName = " dt.TaskEvents";
            p1.Value = combinedOrchestratorMessages.ToTableValueParameter();

            SqlParameter p2 = command.Parameters.Add("@NewHistoryEvents", SqlDbType.Structured);
            p2.TypeName = " dt.TaskEvents";
            p2.Value = newEvents.ToTableValueParameter(instance, nextSequenceNumber);

            SqlParameter p3 = command.Parameters.Add("@NewTaskEvents", SqlDbType.Structured);
            p3.TypeName = " dt.TaskEvents";
            p3.Value = dbTaskEvents.ToTableValueParameter();

            SqlParameter p4 = command.Parameters.Add("@UpdatedInstanceStatus", SqlDbType.Structured);
            p4.TypeName = " dt.TaskEvents";
            p4.Value = orchestrationState.ToTableValueParameter(lockExpiration: null);

            SqlParameter p5 = command.Parameters.Add("@DeletedControlMessages", SqlDbType.Structured);
            p5.TypeName = " dt.TaskEvents";
            p5.Value = workItem.NewMessages.ToTableValueParameter(onlyMessageId: true);

            DbDataReader reader;

            try
            {
                reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper, timeout.Token);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, instance);
                throw;
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

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem) => Task.CompletedTask;

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
            command.CommandText = "dt.LockNextTask";
            command.AddParameter("@LockedBy", lockedBy);
            command.AddParameter("@LockExpiration", lockExpiration);

            DbDataReader reader;

            try
            {
                reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper, shutdownToken);
            }
            catch (Exception e)
            {
                this.traceHelper.ProcessingError(e, new OrchestrationInstance());
                throw;
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
            return Task.CompletedTask; // TODO
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
            command.CommandText = "dt.CompleteTasks";

            SqlParameter p1 = command.Parameters.Add("@CompletedTasks", SqlDbType.Structured);
            p1.TypeName = " dt.TaskEvents";
            p1.Value = workItem.TaskMessage.ToTableValueParameter(onlyMessageId: true);

            SqlParameter p2 = command.Parameters.Add("@Results", SqlDbType.Structured);
            p2.TypeName = " dt.TaskEvents";
            p2.Value = responseMessage.ToTableValueParameter();

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, timeout.Token);
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

        sealed class DatabaseLock : IAsyncDisposable
        {
            readonly SqlConnection connection;
            readonly SqlTransaction transaction;

            bool committed;

            public DatabaseLock(SqlConnection connection, SqlTransaction transaction)
            {
                this.connection = connection;
                this.transaction = transaction;
            }

            public bool IsHeld => !this.committed;

            public SqlCommand CreateCommand()
            {
                SqlCommand command = this.connection.CreateCommand();
                command.Transaction = this.transaction;
                return command;
            }

            public Task CommitAsync()
            {
                this.committed = true;
                return this.transaction.CommitAsync();
            }

            public async ValueTask DisposeAsync()
            {
                if (!this.committed)
                {
                    await this.transaction.RollbackAsync();
                }

                await this.connection.CloseAsync();
            }
        }
    }
}
