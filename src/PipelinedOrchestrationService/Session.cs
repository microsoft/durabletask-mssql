// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using global::PipelinedOrchestrationService.WorkItems;

    class Session
    {
        readonly BasicOrchestrationService service;
        readonly string instanceId;

        List<TaskMessage> waiting;
        List<TaskMessage>? processing;

        InstanceConnection? connection;   

        public Session(BasicOrchestrationService service, string instanceId)
        {
            this.service = service;
            this.instanceId = instanceId;
            this.waiting = new List<TaskMessage>();
        }

        public string InstanceId => this.instanceId;

        public bool SessionIsStopping { get; set; }

        public bool ResultPending { get; set; }

        public List<TaskMessage> WaitingMessages => this.waiting;


        public void AddMessage(TaskMessage taskMessage)
        {
            this.waiting.Add(taskMessage);
        } 
      
        public void ContinueIfNotAlreadyPending()
        {  
            if (!this.ResultPending && this.waiting.Count > 0)
            {
                Task _ = this.ProcessNextEpisodeAsync();
            }
        }

        public void CompletePendingAndContinue(IEnumerable<TaskMessage>? requeue)
        {
            Debug.Assert(this.ResultPending);
            this.ResultPending = false;

            if (requeue != null)
            {
                var list = requeue.ToList(); // makes a copy
                list.AddRange(this.waiting);
                this.waiting = list;
            }

            if (this.waiting.Count > 0)
            {
                Task _ = this.ProcessNextEpisodeAsync();
            }
        }

        public async Task ProcessNextEpisodeAsync()
        {
            this.ResultPending = true;

            Debug.Assert(this.processing == null);
            this.processing = this.waiting;
            this.waiting = new List<TaskMessage>();

            InstanceConnection.ExecutionResult executionResult;

            var connection = this.connection;

            if (connection == null)
            {
                connection = await this.service.GetInstanceConnectionAsync(this.instanceId).ConfigureAwait(false);
                
                executionResult = await connection.ProcessHistoryAndMessagesAsync(this.service.LoadHistoryAsync(this.instanceId), this.processing).ConfigureAwait(false);

                this.connection = connection; 
            }
            else
            {
                executionResult = await connection.ProcessMessagesAsync(this.processing).ConfigureAwait(false);
            }    
        }         
    }
}
