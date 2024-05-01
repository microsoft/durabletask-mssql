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

    class Activities : PartitionedTable<long, TaskMessage, SqlCheckpointCommand>
    {
        readonly SqlStore store;

        public Activities(SqlStore store)
        {
            this.store = store;
        }

        public event Action<TaskMessage>? OnActivity;

        public void AddNewActivityToBeProcessed(TxContext tx, TaskMessage taskMessage)
        {         
            long id = tx.GetNextSequenceNumber();

            base.CreateNonExistingRow(tx, id, taskMessage);

            if (this.OnActivity != null)
            {
                tx.WhenCompleted(() =>
                {
                    taskMessage.SequenceNumber = id;
                    this.OnActivity(taskMessage);
                });
            }
        }

        public void RemoveProcessedActivity(TxContext tx, long id)
        {
            base.DeleteExistingRow(tx, id);
        }

        protected override Task<(bool exists, TaskMessage? value)> LoadAsync(long key)
        {
            // TODO
            throw new NotImplementedException();
        }

        protected override void AddDeltaToCheckpointCommand(SqlCheckpointCommand command, Writeback writeback, int partitionId, long key, TaskMessage? Current)
        {
            // TODO
            throw new NotImplementedException();
        }
    }
}
