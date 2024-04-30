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

    class Messages : Table<long, TaskMessage, SqlCheckpointCommand>
    {
        readonly SqlStore store;

        public Messages(SqlStore store)
        {
            this.store = store;
        }

        public event Action<TaskMessage>? OnNewMessage;

        public void AddNewMessageToBeProcessed(TxContext tx, TaskMessage taskMessage)
        {
            long id = tx.GetNextSequenceNumber();

            base.CreateNonExistingRow(tx, id, taskMessage);

            if (this.OnNewMessage != null)
            {
                tx.WhenCompleted(() =>
                {
                    taskMessage.SequenceNumber = id;
                    this.OnNewMessage(taskMessage);
                });
            }
        }

        public void RemoveProcessedMessage(TxContext tx, long id)
        {
            base.DeleteExistingRow(tx, id);
        }

        protected override Task<(bool exists, TaskMessage? value)> LoadAsync(long key)
        {
            // TODO
            throw new NotImplementedException();
        }

        protected override void AddDeltaToCheckpointCommand(SqlCheckpointCommand command, Writeback writeback, long key, TaskMessage? Current)
        {
            // TODO
            throw new NotImplementedException();
        }

    }
}
