// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public abstract class OrchestrationServiceBase : IOrchestrationService, IOrchestrationServiceClient
    {
        /// <summary>
        /// Gets the number of concurrent orchestration dispatchers for fetching orchestration work items.
        /// </summary>
        public virtual int TaskOrchestrationDispatcherCount => 1;

        /// <summary>
        /// Gets the number of concurrent activity dispatchers for fetching activity work items.
        /// </summary>
        public virtual int TaskActivityDispatcherCount => 1;

        public virtual int MaxConcurrentTaskOrchestrationWorkItems 
            => Environment.ProcessorCount;

        public virtual int MaxConcurrentTaskActivityWorkItems 
            => Environment.ProcessorCount;

        public virtual BehaviorOnContinueAsNew EventBehaviourForContinueAsNew
            => BehaviorOnContinueAsNew.Carryover;

        public virtual Task CreateAsync()
            => this.CreateAsync(recreateInstanceStore: false);

        public abstract Task CreateIfNotExistsAsync();

        public abstract Task CreateAsync(bool recreateInstanceStore);

        public virtual Task StartAsync() => Task.CompletedTask;

        public virtual Task StopAsync() => this.StopAsync(isForced: false);

        public virtual Task StopAsync(bool isForced) => Task.CompletedTask;

        public virtual Task DeleteAsync()
            => this.DeleteAsync(deleteInstanceStore: true);

        public abstract Task DeleteAsync(bool deleteInstanceStore);

        public abstract Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken);

        public abstract Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);

        public virtual bool IsMaxMessageCountExceeded(
            int currentMessageCount,
            OrchestrationRuntimeState runtimeState) => false;

        public abstract Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState);

        public virtual Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
            => Task.CompletedTask;

        public abstract Task AbandonTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem);

        public abstract Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken);

        public abstract Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);

        public abstract Task CompleteTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem,
            TaskMessage responseMessage);

        public abstract Task AbandonTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem);

        public virtual int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return exception is OperationCanceledException ? 0 : 1;
        }

        public virtual int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return exception is OperationCanceledException ? 0 : 1;
        }

        public virtual Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
            => this.CreateTaskOrchestrationAsync(creationMessage, Array.Empty<OrchestrationStatus>());

        public abstract Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses);

        public abstract Task SendTaskOrchestrationMessageAsync(TaskMessage message);

        public virtual Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
            => Task.WhenAll(messages.Select(msg => this.SendTaskOrchestrationMessageAsync(msg)));

        public abstract Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken);

        public abstract Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason);

        public virtual async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            OrchestrationState? state = await this.GetOrchestrationStateAsync(instanceId, executionId: null);
            if (state == null)
            {
                return Array.Empty<OrchestrationState>();
            }

            return new[] { state };
        }

        public abstract Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId);

        public virtual Task<string> GetOrchestrationHistoryAsync(string instanceId, string? executionId)
            => throw new NotImplementedException();

        public abstract Task PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }
}
