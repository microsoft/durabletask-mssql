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
        readonly List<PipelinePersistentCache.PartitionMetaData> partitionMetaData;
        readonly List<Action> postCheckpointActions;

        internal SqlCommand Command { get; }


        public SqlCheckpointCommand(SqlStore store)
        {
            this.store = store;
            this.Command = new SqlCommand(); // TODO
            this.partitionMetaData = new();
            this.postCheckpointActions = new();
        }

        public override void AddPostCheckpointAction(Action action)
        {
            this.postCheckpointActions.Add(action);
        }

        public override void SetPartitionMetaData(PipelinePersistentCache.PartitionMetaData partitionMetaData)
        {
            this.partitionMetaData.Add(partitionMetaData);
        }

        public async Task WriteToStorageAsync()
        {
            // .. this.Command. add assignment of sequence counter


            //TODO execute the transactions for all the deltas as a single transaction to SQL database 


            // .. this.Command.ExecuteAsync???
        }

        public void ExecutePostPersistenceActions()
        {
            foreach (var action in this.postCheckpointActions)
            {
                action();
            }
        }
    }
}