// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService.WorkItems
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// represents a connection to a worker for executing a particular orchestration or entity instance. 
    /// this is a rough representation of what we would use with a grpc connection. I just needed something modeled to get started.
    /// It is not quite right because the connection could be initiating stuff on its own.
    /// </summary>
    public abstract class InstanceConnection
    {
        /// <summary>
        /// Asks the worker to process the history and messages, and return the execution result. 
        /// </summary>
        /// <param name="historyTask">A history to (re-)initialize the orchestration with</param>
        /// <param name="messages">New messages to deliver to this orchestration.</param>
        /// <returns></returns>
        public abstract Task<ExecutionResult> ProcessHistoryAndMessagesAsync(Task<IList<HistoryEvent>?> historyTask, IEnumerable<TaskMessage> messages);

        /// <summary>
        /// Asks the worker to process more messages, continuing where it left off previously, and return the execution result. 
        /// </summary>
        /// <param name="history">A history to (re-)initialize the orchestration with</param>
        /// <param name="messages">New messages to deliver to this orchestration.</param>
        /// <returns></returns>
        public abstract Task<ExecutionResult> ProcessMessagesAsync(IEnumerable<TaskMessage> messages);

        /// <summary>
        /// Called when the connection is no longer needed. Once the connection is closed, the client will release its cached cursor, if not already done.
        /// </summary>
        /// <param name="reason">An explanation for why the connection is being closed.</param>
        public abstract Task CloseAsync(string reason);

        /// <summary>
        /// The result of executing an episode of an orchestration.
        /// </summary>
        /// <param name="InstanceUpdate">How to update the instance state.</param>
        /// <param name="ActivityMessages">Any activity messages that were produced.</param>
        /// <param name="InstanceMessages">Any instance messages that were produced.</param>
        /// <param name="DelayedMessages">Any delayed messages that were produced.</param>
        public record ExecutionResult(
           InstanceUpdate InstanceUpdate,
           IEnumerable<TaskMessage> ActivityMessages,
           IEnumerable<TaskMessage> InstanceMessages,
           IEnumerable<TaskMessage> DelayedMessages
        );

        /// <summary>
        /// Records that describe how the instance state should be updated after executing an episode of an orchestration.
        /// </summary>
        public record InstanceUpdate
        {
            /// <summary>
            /// The history grew larger.
            /// </summary>
            /// <param name="AppendedEvents">The new events to append.</param>
            /// <param name="State">The new instance state.</param>
            public record AppendHistoryEvents(int Position, IList<HistoryEvent> AppendedEvents, OrchestrationState State) : InstanceUpdate;

            /// <summary>
            /// The history was replaced by a new history.
            /// </summary>
            /// <param name="Events">The events of the new history.</param>
            /// <param name="State">The new instance state.</param>
            public record NewHistory(IList<HistoryEvent> Events, OrchestrationState State) : InstanceUpdate;

            /// <summary>
            /// The messages were discarded, and the instance state and history remain unchanged.
            /// </summary>
            /// <param name="Reason">The reason why the messages were discarded.</param>
            public record DiscardMessages(string Reason) : InstanceUpdate;

            /// <summary>
            /// Execution failed transiently and the result should be considered a no-op.
            /// </summary>
            public record Disconnected(string Reason) : InstanceUpdate;

            /// <summary>
            /// The instance was deleted after processing the messages.
            /// </summary>
            public record DeleteInstance() : InstanceUpdate;
        }

    }
}
