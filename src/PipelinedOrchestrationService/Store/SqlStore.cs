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
    using DurableTask.SqlServer;
    using Microsoft.Data.SqlClient;
    using PipelinePersistentCache;

    /// <summary>
    /// encapsulates the SQL storage interface and operations.
    /// </summary>
    public class SqlStore
    {
        // this stuff is just copied from SqlOrchestrationService
        // and no more than a rough placeholder right now

        readonly CancellationToken shutdownToken;
        readonly PipelinePersistentCache cache;
        readonly CheckpointWorker checkpointWorker;
        readonly SqlOrchestrationServiceSettings settings;
        readonly LogHelper traceHelper;
        readonly SqlDbManager dbManager;
        readonly string lockedByValue;
        readonly string userId;

        public SqlDbManager DbManager => this.dbManager;

        public SqlStore(SqlOrchestrationServiceSettings? settings, PipelinePersistentCache cache, CancellationToken shutdownToken)
        {
            this.cache = cache; 
            this.shutdownToken = shutdownToken;
            this.settings = SqlOrchestrationService.ValidateSettings(settings) ?? throw new ArgumentNullException(nameof(settings));

            this.traceHelper = new LogHelper(this.settings.LoggerFactory.CreateLogger("DurableTask.SqlServer"));
            this.dbManager = new SqlDbManager(this.settings, this.traceHelper);
            this.lockedByValue = $"{this.settings.AppName},{Process.GetCurrentProcess().Id}";
            this.userId = new SqlConnectionStringBuilder(this.settings.TaskHubConnectionString).UserID ?? string.Empty;

            this.checkpointWorker = new CheckpointWorker(this, shutdownToken);

            foreach(int partitionId in cache.OwnedPartitions)
            {
                cache.AddTransactionCompletionListener(partitionId, (long txId) => this.checkpointWorker.Notify());
            }
        }
        
        class CheckpointWorker: BatchWorker<object>
        {
            readonly SqlStore store;

            public CheckpointWorker(SqlStore store, CancellationToken cancellation) : base(false, 10000, cancellation)
            {
                this.store = store;
            }

            protected override async ValueTask Process(IList<object> _)
            {
                SqlCheckpointCommand checkpoint = new SqlCheckpointCommand(this.store);

                // collect the checkpoint data
                await this.store.cache.CollectNextCheckpointAsync(checkpoint);

                // then write the checkpoint to storage
                await checkpoint.WriteToStorageAsync();

                // finally execute the post-persistence actions
                checkpoint.ExecutePostPersistenceActions();
            }
        }
    }
}
