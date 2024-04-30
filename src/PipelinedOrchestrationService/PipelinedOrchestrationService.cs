// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Numerics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Query;
    using DurableTask.SqlServer;
    using DurableTask.SqlServer.Utils;
    using PipelinePersistentCache;

    public class PipelinedOrchestrationService : OrchestrationServiceBase
    {
        readonly SqlOrchestrationServiceSettings settings;
        readonly SqlStore store;
        readonly PipelinePersistentCache cache;
        readonly BasicOrchestrationService basic;

        public PipelinedOrchestrationService(SqlOrchestrationServiceSettings settings)
        {
            this.settings = settings;
            this.cache = new PipelinePersistentCache();
            this.store = new SqlStore(settings, this.cache, this.ShutdownToken);
            this.basic = new BasicOrchestrationService(this.cache, this.store, this.ShutdownToken);
        }

        public override int MaxConcurrentTaskOrchestrationWorkItems => this.settings.MaxActiveOrchestrations;

        public override int MaxConcurrentTaskActivityWorkItems => this.settings.MaxConcurrentActivities;

        public override Task CreateAsync(bool recreateInstanceStore)
            => this.store.DbManager.CreateOrUpgradeSchemaAsync(recreateInstanceStore);

        public override Task CreateIfNotExistsAsync()
            => this.store.DbManager.CreateOrUpgradeSchemaAsync(recreateIfExists: false);

        public override Task DeleteAsync(bool deleteInstanceStore)
        {
            if (!deleteInstanceStore)
            {
                throw new NotSupportedException($"Setting the {deleteInstanceStore} parameter to false is not supported.");
            }

            return this.store.DbManager.DeleteSchemaAsync();
        }

        public override Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            return this.basic.CompleteTaskActivityWorkItemAsync(workItem, responseMessage, this.ShutdownToken);
        }

        public override Task CompleteTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem, OrchestrationRuntimeState newOrchestrationRuntimeState, IList<TaskMessage> activityMessages, IList<TaskMessage> orchestratorMessages, IList<TaskMessage> delayedMessages, TaskMessage continuedAsNewMessage, OrchestrationState orchestrationState)
        {
            // separate orchestrator messages into delayed messages and instance messages      
            IList<TaskMessage>? instanceMessages = null;
            if (orchestratorMessages != null)
            {
                foreach (TaskMessage taskMessage in orchestratorMessages)
                {              
                    if (DurableTask.Core.Common.Entities.IsDelayedEntityMessage(taskMessage, out _))
                    {
                        (delayedMessages ??= new List<TaskMessage>()).Add(taskMessage);
                    }
                    else if (taskMessage.Event is ExecutionStartedEvent ese && ese.ScheduledStartTime.HasValue)
                    {
                        (delayedMessages ??= new List<TaskMessage>()).Add(taskMessage);
                    }
                    else
                    {
                        (instanceMessages ??= new List<TaskMessage>()).Add(taskMessage);
                    }
                }
            }

            return this.basic.CompleteTaskOrchestrationWorkItemAsync(workItem, newOrchestrationRuntimeState, activityMessages, instanceMessages, delayedMessages, orchestrationState);
        }

        public override Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return this.basic.AbandonTaskOrchestrationWorkItemAsync(workItem);
        }

        public override Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public override Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            return this.basic.CreateTaskOrchestrationAsync(creationMessage, dedupeStatuses, this.ShutdownToken);
        }

        public override Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        public override Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId)
        {
            return this.basic.GetOrchestrationStateAsync(instanceId, executionId, this.ShutdownToken);
        }

        public override Task<OrchestrationQueryResult> GetOrchestrationWithQueryAsync(OrchestrationQuery query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.basic.LockNextTaskActivityWorkItem(receiveTimeout, cancellationToken);
        }

        public override Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.basic.LockNextTaskOrchestrationWorkItemAsync(receiveTimeout, cancellationToken);
        }

        public override Task<PurgeResult> PurgeInstanceStateAsync(string instanceId)
        {
            return this.basic.PurgeInstanceStateAsync(instanceId, this.ShutdownToken);
        }

        public override Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
        {
            throw new NotImplementedException();
        }

        public override Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public override Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            return Task.FromResult(workItem);
        }

        public override Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.CompletedTask;
        }

        public override Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return this.basic.SendTaskOrchestrationMessageAsync(message, this.ShutdownToken);
        }

        public override async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            using var timeoutCts = timeout < TimeSpan.MaxValue && timeout >= TimeSpan.Zero ?
                new CancellationTokenSource(timeout) :
                new CancellationTokenSource();

            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(
                timeoutCts.Token,
                cancellationToken);

            while (true)
            {
                OrchestrationState? state = await this.basic.GetOrchestrationStateAsync(
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
    }
}
