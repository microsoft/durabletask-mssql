// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    class SqlCheckpointCommand : PipelinePersistentCache.CheckpointCommand
    {
        readonly SqlStore store;

        internal SqlCommand Command { get; }

        long sequenceCounter;

        List<Action>? postCheckpointActions;

        public SqlCheckpointCommand(SqlStore store)
        {
            this.store = store;
            this.Command = new SqlCommand(); // TODO
        }

        public override void AddPostCheckpointActions(IEnumerable<Action> actions)
        {
            this.postCheckpointActions ??= new List<Action>();
            this.postCheckpointActions.AddRange(actions);
        }

        public override void SetSequenceCounter(long sequenceCounter)
        {
            this.sequenceCounter = sequenceCounter;
        }

        public async Task WriteToStorageAsync()
        {
            // .. this.Command. add assignment of sequence counter


            //TODO execute the transactions for all the deltas as a single transaction to SQL database 


            // .. this.Command.ExecuteAsync???
        }

        public void ExecutePostPersistenceActions()
        {
            if (this.postCheckpointActions != null)
            {
                foreach (var action in this.postCheckpointActions)
                {
                    action();
                }
            }
        }
    }
}