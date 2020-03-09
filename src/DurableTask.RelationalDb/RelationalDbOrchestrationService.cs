using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using Dynamitey;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableTask.RelationalDb
{
    public abstract class RelationalDbOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient
    {
        readonly BackoffPollingHelper backoffHelper = new BackoffPollingHelper(
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(3)); // TODO: Configurable

        public abstract int MaxOrchestrationConcurrency { get; }

        public abstract int MaxActivityConcurrency { get; }

        public abstract DbConnection GetConnection();

        int IOrchestrationService.TaskOrchestrationDispatcherCount => this.MaxOrchestrationConcurrency;

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => this.MaxOrchestrationConcurrency;

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        int IOrchestrationService.TaskActivityDispatcherCount => this.MaxActivityConcurrency;

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => this.MaxActivityConcurrency;

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
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // TODO: Configurable
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(timeout.Token);

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "CreateInstances";

            SqlParameter parameter = command.Parameters.Add("@NewInstanceEvents", SqlDbType.Structured);
            parameter.TypeName = "TaskEvents";
            parameter.Value = creationMessage.ToTableValueParameter();

            // TODO: support for dedupeStatuses

            await command.ExecuteNonQueryAsync(timeout.Token);
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

                    using SqlDataReader reader = await command.ExecuteReaderAsync(combinedCts.Token);
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

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

            // Result #1: The list of control queue messages
            var messages = new List<TaskMessage>(batchSize);
            while (await reader.ReadAsync(cancellationToken))
            {
                messages.Add(reader.GetTaskMessage());
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

            return new TaskOrchestrationWorkItem
            {
                InstanceId = messages[0].OrchestrationInstance.InstanceId,
                LockedUntilUtc = lockExpiration,
                NewMessages = messages,
                OrchestrationRuntimeState = new OrchestrationRuntimeState(history),
            };
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
            p3.Value = outboundMessages.ToTableValueParameter();

            SqlParameter p4 = command.Parameters.Add("@UpdatedInstanceStatus", SqlDbType.Structured);
            p4.TypeName = "TaskEvents";
            p4.Value = orchestrationState.ToTableValueParameter(lockExpiration: null);

            SqlParameter p5 = command.Parameters.Add("@DeletedControlMessages", SqlDbType.Structured);
            p5.TypeName = "TaskEvents";
            p5.Value = workItem.NewMessages.ToTableValueParameter(onlyMessageId: true);

            await command.ExecuteNonQueryAsync(timeout.Token);

            this.backoffHelper.Reset();
        }

        async Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)); // TODO: Configurable
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(timeout.Token);

            using SqlCommand command = (SqlCommand)connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "CheckpointOrchestration";
        }

        async Task<TaskActivityWorkItem?> IOrchestrationService.LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            using DbConnection connection = this.GetConnection();
            await connection.OpenAsync(cancellationToken);

            DateTime lockExpiration = DateTime.UtcNow.AddMinutes(2); // TODO: Configurable
            string lockedBy = Environment.MachineName; //  TODO: Configurable

            using DbCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "LockNextTask";
            command.AddParameter("@LockedBy", lockedBy);
            command.AddParameter("@LockExpiration", lockExpiration);

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
            {
                await this.backoffHelper.WaitAsync(cancellationToken);
                return null;
            }

            this.backoffHelper.Reset();

            TaskMessage taskMessage = reader.GetTaskMessage();
            return new TaskActivityWorkItem
            {
                Id = $"{taskMessage.OrchestrationInstance.InstanceId}::{taskMessage.SequenceNumber:X8}",
                TaskMessage = taskMessage,
                LockedUntilUtc = lockExpiration,
            };
        }

        Task IOrchestrationService.AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        async Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem,
            TaskMessage responseMessage)
        {
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

            await command.ExecuteNonQueryAsync(timeout.Token);

            this.backoffHelper.Reset();
        }

        Task IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        Task IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
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
