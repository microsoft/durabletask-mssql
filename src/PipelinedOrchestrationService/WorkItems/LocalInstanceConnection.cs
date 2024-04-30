// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using global::PipelinedOrchestrationService.WorkItems;

    /// <summary>
    /// Implements an instance "connection" that is just relying on the local orchestration work item queue.
    /// </summary>
    class LocalInstanceConnection : InstanceConnection
    {
        readonly WorkItemQueue<TaskOrchestrationWorkItem> queue;
        readonly string instanceId;

        OrchestrationWorkItem? cachedCursor;

        public LocalInstanceConnection(WorkItemQueue<TaskOrchestrationWorkItem> queue, string instanceId)
        {
            this.queue = queue;
            this.instanceId = instanceId;
        }

        public override Task CloseAsync(string reason)
        {
            // no need to do anything for in-process
            return Task.CompletedTask;
        }

        public override async Task<ExecutionResult> ProcessHistoryAndMessagesAsync(Task<IList<HistoryEvent>?> historyTask, IEnumerable<TaskMessage> messages)
        {
            var history = await historyTask; 
               
            this.cachedCursor = new OrchestrationWorkItem(this)
            {
                InstanceId = this.instanceId,
                OrchestrationRuntimeState = history?.Count > 0 ? new OrchestrationRuntimeState(history) : new OrchestrationRuntimeState(),
                LockedUntilUtc = DateTime.MaxValue,
            };

            return await this.ProcessMessagesAsync(messages);
        }

        public override async Task<ExecutionResult> ProcessMessagesAsync(IEnumerable<TaskMessage> messages)
        {
            Task<ExecutionResult> resultTask = this.cachedCursor!.SetNextMessageBatch(messages);

            if (!resultTask.IsCompleted)
            {
                this.queue.Add(this.cachedCursor);
            }

            var episodeResult = await resultTask;

            return episodeResult;
        }

        public interface IOrchestrationWorkItem
        {
            void OnCompleted(
                OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage>? activityMessages,
                IList<TaskMessage>? instanceMessages,
                IList<TaskMessage>? delayedMessages,
                OrchestrationState state);

            void OnAbandoned();
        }

        readonly static IEnumerable<TaskMessage> emptyMessages = Enumerable.Empty<TaskMessage>();

        class OrchestrationWorkItem : TaskOrchestrationWorkItem, IOrchestrationWorkItem
        {
            readonly InstanceConnection connection; // the connection that owns this work item

            TaskCompletionSource<ExecutionResult>? taskCompletionSource;
            string? executionId;

            public OrchestrationWorkItem(InstanceConnection connection)
            {
                this.connection = connection;
            }

            public Task<ExecutionResult> SetNextMessageBatch(IEnumerable<TaskMessage> batch)
            {
                this.NewMessages = batch.ToList();
                this.OrchestrationRuntimeState.NewEvents.Clear();

                if (!IsExecutableInstance(out string reason))
                {
                    return Task.FromResult(new ExecutionResult(
                        InstanceUpdate: new InstanceUpdate.DiscardMessages(reason),
                        ActivityMessages: emptyMessages,
                        InstanceMessages: emptyMessages,
                        DelayedMessages: emptyMessages
                    ));
                }
                else
                {
                    this.taskCompletionSource = new TaskCompletionSource<ExecutionResult>();
                    return this.taskCompletionSource.Task;
                }

                bool IsExecutableInstance(out string message)
                {
                    message = string.Empty;

                    if (this.OrchestrationRuntimeState.ExecutionStartedEvent == null
                        && !this.NewMessages.Any(msg => msg.Event is ExecutionStartedEvent))
                    {
                        if (DurableTask.Core.Common.Entities.AutoStart(this.InstanceId, this.NewMessages))
                        {
                            return true;
                        }
                        else
                        {
                            if (DurableTask.Core.Common.Entities.IsEntityInstance(this.InstanceId))
                            {
                                message = "Instance is an entity that cannot process this type of message";
                            }
                            else if (this.OrchestrationRuntimeState.Events.Count == 0)
                            {
                                message = "Instance does not exist (it may have been purged)";
                            }
                            else
                            {
                                message = $"Instance has an invalid history ({this.OrchestrationRuntimeState.Events} events)";
                            }
                            return false;
                        }
                    }

                    if (this.OrchestrationRuntimeState.ExecutionStartedEvent != null &&
                        this.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                        this.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Pending)
                    {
                        message = $"Instance is {this.OrchestrationRuntimeState.OrchestrationStatus}";
                        return false;
                    }

                    return true;
                }
            }

            public void OnCompleted(
                OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage>? activityMessages,
                IList<TaskMessage>? instanceMessages,
                IList<TaskMessage>? delayedMessages,
                OrchestrationState state)
            {
                InstanceUpdate instanceUpdate;

                if (newOrchestrationRuntimeState.Events.Count == 3
                   && newOrchestrationRuntimeState.Events[0].EventType == EventType.OrchestratorStarted
                   && newOrchestrationRuntimeState.Events[1] is ExecutionStartedEvent executionStartedEvent
                   && executionStartedEvent.Input == null
                   && DurableTask.Core.Common.Entities.IsEntityInstance(executionStartedEvent.OrchestrationInstance.InstanceId)
                   && newOrchestrationRuntimeState.Events[2].EventType == EventType.OrchestratorCompleted)
                {
                    instanceUpdate = new InstanceUpdate.DeleteInstance(); // history represents a deleted entity
                    this.executionId = null;
                }
                else if (this.executionId != null && this.executionId != newOrchestrationRuntimeState.OrchestrationInstance?.ExecutionId)
                {
                    Debug.Assert(newOrchestrationRuntimeState.Events.Count == 0);
                    instanceUpdate = new InstanceUpdate.NewHistory(
                       Events: (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                       State: state);
                    this.executionId = newOrchestrationRuntimeState.OrchestrationInstance?.ExecutionId;
                }
                else
                {
                    instanceUpdate = new InstanceUpdate.AppendHistoryEvents(
                        Position: newOrchestrationRuntimeState.Events.Count,
                        AppendedEvents: (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                        State: state);
                    this.executionId = newOrchestrationRuntimeState.OrchestrationInstance?.ExecutionId;
                }

                this.taskCompletionSource!.TrySetResult(new ExecutionResult(
                        InstanceUpdate: new InstanceUpdate.DeleteInstance(),
                        ActivityMessages: activityMessages ?? emptyMessages,
                        InstanceMessages: instanceMessages ?? emptyMessages,
                        DelayedMessages: delayedMessages ?? emptyMessages
                    ));
            }

            public void OnAbandoned()
            {
                this.taskCompletionSource!.TrySetResult(new ExecutionResult(
                        InstanceUpdate: new InstanceUpdate.Disconnected("work item was abandoned"),
                        ActivityMessages: emptyMessages,
                        InstanceMessages: emptyMessages,
                        DelayedMessages: emptyMessages
                    ));
            }
        }
    }
}
