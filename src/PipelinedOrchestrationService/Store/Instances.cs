// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using PipelinePersistentCache;

    class Instances : PartitionedTable<string, Instances.Info, SqlCheckpointCommand>
    {
        readonly SqlStore store;

        public Instances(SqlStore store)
        {
            this.store = store;
        }

        public readonly record struct Info(
            // TODO some stuff is missing here, requires some more care before finalizing
            string ExecutionId, 
            string Name, 
            string Version, 
            OrchestrationStatus Status, 
            string Input, 
            string Output, 
            string CustomStatus, 
            DateTime LastUpdatedTime, 
            DateTime CreatedTime); 


        public void EnsureInMemory(TxContext tx, string instanceId)
        {
            base.PrefetchRow(tx, instanceId);
        }

        public OrchestrationState? GetState(TxContext tx, string instanceId)
        {
            if (base.TryGetRow(tx, instanceId, out var info))
            {
                return new OrchestrationState()
                {                    
                    CreatedTime = info.CreatedTime,
                    Input = info.Input,
                    LastUpdatedTime = info.LastUpdatedTime,
                    Version = info.Version,
                    Status = info.CustomStatus,
                    OrchestrationStatus = info.Status,
                    Name = info.Name,
                    OrchestrationInstance = new OrchestrationInstance() { InstanceId = instanceId, ExecutionId = info.ExecutionId },
                    Output = info.Output
                };
            }
            else
            {
                return null;
            }
        }

        public bool Exists(TxContext tx, string instanceId)
        {
            return base.TryGetRow(tx, instanceId, out _);
        }

        public void SetState(TxContext tx, string instanceId, OrchestrationState state)
        {
            base.UpdateExistingRow(tx, instanceId, new Info()
            {
                // TODO some stuff is missing here, requires some more care before finalizing
                CreatedTime = state.CreatedTime,
                Input = state.Input,
                LastUpdatedTime = state.LastUpdatedTime,
                Version = state.Version,
                CustomStatus = state.Status,
                Status = state.OrchestrationStatus,
                Name = state.Name,
                ExecutionId = state.OrchestrationInstance.ExecutionId,
                Output = state.Output
            });
        }

        public void DeleteState(TxContext tx, string instanceId)
        {
            base.DeleteExistingRow(tx, instanceId);
        }

        protected override Task<(bool exists, Info value)> LoadAsync(string key)
        {
            //TODO
            throw new NotImplementedException();
        }

        protected override void AddDeltaToCheckpointCommand(SqlCheckpointCommand command, Writeback writeback, int partitionId, string key, Info Current)
        {
            //TODO
            throw new NotImplementedException();
        }
    }
}
