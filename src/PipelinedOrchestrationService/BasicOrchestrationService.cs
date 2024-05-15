// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using global::PipelinedOrchestrationService.WorkItems;
    using PipelinePersistentCache;

    class BasicOrchestrationService
    {
        readonly SqlStore store;
        readonly Activities activities;
        readonly Histories histories;
        readonly Instances instances;
        readonly Messages messages;
        readonly uint totalPartitions;

        readonly PipelinePersistentCache cache;

        readonly ConcurrentDictionary<(int,string), Session> sessions;

        readonly WorkItemQueue<TaskActivityWorkItem> activityWorkItemQueue;
        readonly WorkItemQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue;

        readonly CancellationToken shutdownToken;

        
        public BasicOrchestrationService(PipelinePersistentCache cache, SqlStore store, CancellationToken shutdownToken)
        {
            this.cache = cache;
            this.store = store;
            this.activities = new Activities(store);
            this.histories = new Histories(store);   
            this.instances = new Instances(store);
            this.messages = new Messages(store); 
            this.sessions = new();
            this.activityWorkItemQueue = new WorkItemQueue<TaskActivityWorkItem>();
            this.orchestrationWorkItemQueue = new WorkItemQueue<TaskOrchestrationWorkItem>();
            this.shutdownToken = shutdownToken;

            this.totalPartitions = (uint) cache.TotalPartitions;

            // we hook up callbacks to be invoked whenever messages or activities arrive
            // these same callbacks are also called while 
            this.messages.OnNewMessage += this.OnNewMessage;
            this.activities.OnActivity += this.OnNewActivity;
        }

       
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses, CancellationToken cancellationToken)
        {
            string instanceId = creationMessage.OrchestrationInstance.InstanceId;
            var partitionId = (int) PartitionHash.GetPartitionId(instanceId, this.totalPartitions);
 
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);

            // we must bring the instance state into memory prior to doing the transaction
            this.instances.EnsureInMemory(tx, instanceId);
            this.histories.EnsureInMemory(tx, instanceId);
            await tx.CompletePrefetchesAsync();

            OrchestrationState? instanceState = this.instances.GetState(tx, instanceId);
            OrchestrationCreation.Result creationResult = OrchestrationCreation.HandleCreationRequest(instanceState, creationMessage, dedupeStatuses);

            switch(creationResult)
            {
                case OrchestrationCreation.Result.Deduplicate deduplicate:
                    tx.Commit();
                    throw new OrchestrationAlreadyExistsException($"An instance with status {deduplicate.ExistingStatus} already exists");

                case OrchestrationCreation.Result.NewOrReplace newOrReplace:
                    this.instances.SetState(tx, instanceId, newOrReplace.State);
                    this.histories.SetHistory(tx, instanceId, new HistoryEvent[0]);
                    this.messages.AddNewMessageToBeProcessed(tx, creationMessage);
                    tx.Commit();
                    break;
            }
        }

        void OnNewMessage(TaskMessage taskMessage)
        {
            string instanceId = taskMessage.OrchestrationInstance.InstanceId;
            var partitionId = (int)PartitionHash.GetPartitionId(instanceId, this.totalPartitions);

            this.sessions.AddOrUpdate((partitionId, instanceId), CreateSession, UpdateSession);

            // the functions below are executed under the implicit "lock" of the concurrent dictionary    
            // which serves as our concurrency control on modifying the state on a session

            Session CreateSession((int, string) _)
            {
                var session = new Session(this, partitionId, instanceId);
                session.AddMessage(taskMessage);
                session.ContinueIfNotAlreadyPending();
                return session;
            }

            Session UpdateSession((int, string) _, Session session)
            {
                if (!session.SessionIsStopping)
                {
                    session.AddMessage(taskMessage);
                    session.ContinueIfNotAlreadyPending();
                    return session;
                }
                else
                {
                    // we are just one step ahead of a racing TryRemove operation. 
                    // To keep things correct we change the session so the TryRemove will become a no-op.
                    session = new Session(this, partitionId, instanceId);
                    session.AddMessage(taskMessage);
                    session.ContinueIfNotAlreadyPending();
                    return session;
                }
            }     
        }

        void OnNewActivity(TaskMessage taskMessage)
        {
            this.activityWorkItemQueue.Add(new TaskActivityWorkItem()
            {
                Id = taskMessage.SequenceNumber.ToString(),
                LockedUntilUtc = DateTime.MaxValue,
                TaskMessage = taskMessage,
            });
        }

        void ContinueSessionAfterReceivingResult(Session session, IEnumerable<TaskMessage>? requeue = null)
        {
            this.sessions.AddOrUpdate(session.Key, CreateSession, UpdateSession);

            // the functions below are executed under the implicit "lock" of the concurrent dictionary    
            // which serves as our concurrency control on modifying the state on a session 

            Session CreateSession((int, string) _)
            {
                // we don't remove sessoins while they have pending requests
                throw new InvalidOperationException("internal error - session was removed");              
            }

            Session UpdateSession((int, string) _, Session session1)
            {
                Debug.Assert(session == session1);
                session.CompletePendingAndContinue(requeue);
                return session;
            }
        }

        public async Task ProcessExecutionResultAsync(Session session, InstanceConnection connection, InstanceConnection.ExecutionResult executionResult, IEnumerable<TaskMessage> processedMessages)
        {
            string instanceId = session.InstanceId;

            // ---  first, check if the completion was anomalous (discarded, abandoned) 
            
            if (executionResult.InstanceUpdate is InstanceConnection.InstanceUpdate.Disconnected disconnected)
            {
                // todo implement closing
                //  await session.CloseConnectionAsync(disconnected.Reason);
                this.ContinueSessionAfterReceivingResult(session, requeue: processedMessages);
                return;
            }

            if (executionResult.InstanceUpdate is InstanceConnection.InstanceUpdate.DiscardMessages discardMessages)
            {
                this.ContinueSessionAfterReceivingResult(session);
                return;
            }

            // --- otherwise, we process the completion result by transactionally updating all relevant state

            using TxContext tx = await this.cache.StartTransactionAsync(session.PartitionId);
            this.instances.EnsureInMemory(tx, instanceId);
            this.histories.EnsureInMemory(tx, instanceId);
            await tx.CompletePrefetchesAsync();
            switch (executionResult.InstanceUpdate)
            {
                case InstanceConnection.InstanceUpdate.AppendHistoryEvents appendHistoryEvents:

                    this.instances.SetState(tx, instanceId, appendHistoryEvents.State);
                    this.histories.AppendHistory(tx, instanceId, appendHistoryEvents.Position, appendHistoryEvents.AppendedEvents);
                    break;

                case InstanceConnection.InstanceUpdate.NewHistory newHistory:

                    this.instances.SetState(tx, instanceId, newHistory.State);
                    this.histories.SetHistory(tx, instanceId, newHistory.Events);
                    break;

                case InstanceConnection.InstanceUpdate.DeleteInstance:

                    this.instances.DeleteState(tx, instanceId);
                    this.histories.DeleteHistory(tx, instanceId);
                    break;
            }

            foreach(var taskMessage in executionResult.ActivityMessages)
            {
                this.activities.AddNewActivityToBeProcessed(tx, taskMessage);
            }

            foreach (var taskMessage in executionResult.InstanceMessages)
            {
                this.messages.AddNewMessageToBeProcessed(tx, taskMessage);
            }

            foreach (var taskMessage in executionResult.DelayedMessages)
            {
                // this.timers.Add
                throw new NotImplementedException("TODO");
            }

            foreach (var taskMessage in processedMessages)
            {
                this.messages.RemoveProcessedMessage(tx, taskMessage.SequenceNumber);
            }

            tx.Commit();
        }

        public async Task<OrchestrationState?> GetOrchestrationStateAsync(string instanceId, string? executionId, CancellationToken cancellation)
        {
            var partitionId = (int)PartitionHash.GetPartitionId(instanceId, this.totalPartitions);
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);
            this.instances.EnsureInMemory(tx, instanceId);
            await tx.CompletePrefetchesAsync();
            OrchestrationState? instanceState = this.instances.GetState(tx, instanceId);
            tx.Commit();
            return instanceState;
        }

        internal async Task<IList<HistoryEvent>?> LoadHistoryAsync(string instanceId)
        {
            var partitionId = (int)PartitionHash.GetPartitionId(instanceId, this.totalPartitions);
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);
            this.histories.EnsureInMemory(tx, instanceId);
            await tx.CompletePrefetchesAsync().ConfigureAwait(false);
            var history = this.histories.GetHistory(tx, instanceId);
            tx.Commit();
            return history;
        }

        internal Task<InstanceConnection> GetInstanceConnectionAsync(string instanceId)
        {
            return Task.FromResult((InstanceConnection) new LocalInstanceConnection(this.orchestrationWorkItemQueue, instanceId));
        }

        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message, CancellationToken cancellationToken)
        {
            var partitionId = (int)PartitionHash.GetPartitionId(message.OrchestrationInstance.InstanceId, this.totalPartitions);
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);
            this.messages.AddNewMessageToBeProcessed(tx, message);
            tx.Commit();
        }

        public async Task<PurgeResult> PurgeInstanceStateAsync(string instanceId, CancellationToken cancellationToken)
        {
            var partitionId = (int)PartitionHash.GetPartitionId(instanceId, this.totalPartitions);
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);
            this.instances.EnsureInMemory(tx, instanceId);
            this.histories.EnsureInMemory(tx, instanceId);
            await tx.CompletePrefetchesAsync();
            if (this.instances.Exists(tx, instanceId))
            {
                this.instances.DeleteState(tx, instanceId);
                this.histories.DeleteHistory(tx, instanceId);
                tx.Commit();
                return new PurgeResult(1);
            }
            else
            {
                tx.Commit();
                return new PurgeResult(0);
            }       
        }

        public Task<TaskActivityWorkItem?> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.activityWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        public Task<TaskOrchestrationWorkItem?> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.orchestrationWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        string GetActivityWorkItemId(int partition, long sequenceNumber)
        {
            return $"A{sequenceNumber}P{partition}";
        }
        void ParseActivityWorkItemId(string id, out int partition, out long sequenceNumber)
        {
            int pos = id.LastIndexOf('P');
            sequenceNumber = long.Parse(id.Substring(1, pos - 1));
            partition = int.Parse(id.Substring(pos + 1)); 
        }

        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage, CancellationToken cancellationToken)
        {
            this.ParseActivityWorkItemId(workItem.Id, out int partitionId, out long sequenceNumber);
            using TxContext tx = await this.cache.StartTransactionAsync(partitionId);
            this.activities.RemoveProcessedActivity(tx, sequenceNumber);
            this.messages.AddNewMessageToBeProcessed(tx, responseMessage);
            tx.Commit();
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage>? activityMessages,
            IList<TaskMessage>? instanceMessages,
            IList<TaskMessage>? delayedMessages,
            OrchestrationState state)
        {
            // give the execution result to the connection so it can give it back to the requestor
            ((LocalInstanceConnection.IOrchestrationWorkItem)workItem)
                .OnCompleted(newOrchestrationRuntimeState, activityMessages, instanceMessages, delayedMessages, state);

            return Task.CompletedTask;
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // give the execution result to the connection so it can give it back to the requestor
            ((LocalInstanceConnection.IOrchestrationWorkItem)workItem)
                .OnAbandoned();

            return Task.CompletedTask;
        }
    }
}
