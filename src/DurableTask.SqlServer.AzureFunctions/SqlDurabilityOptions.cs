// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Newtonsoft.Json;

    class SqlDurabilityOptions
    {
        [JsonProperty("connectionStringName")]
        public string ConnectionStringName { get; set; } = "SQLDB_Connection";

        [JsonProperty("taskHubName")]
        public string TaskHubName { get; set; } = "default";

        [JsonProperty("taskEventLockTimeout")]
        public TimeSpan TaskEventLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        [JsonProperty("taskEventBatchSize")]
        public int TaskEventBatchSize { get; set; } = 10;

        [JsonProperty("createDatabaseIfNotExists")]
        public bool CreateDatabaseIfNotExists { get; set; }

        [JsonProperty("schemaName")]
        public string? SchemaName { get; set; }

        internal ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;
        
        internal SqlOrchestrationServiceSettings GetOrchestrationServiceSettings(
            DurableTaskOptions extensionOptions,
            IConnectionInfoResolver connectionStringResolver)
        {
            if (connectionStringResolver == null)
            {
                throw new ArgumentNullException(nameof(connectionStringResolver));
            }

            // If SchemaName is wrapped in `%`, then it is an environment variable to be resolved
            if (this.SchemaName != null && this.SchemaName.StartsWith("%") && this.SchemaName.EndsWith("%"))
            {
                // remove surrounding `%` characters
                this.SchemaName = this.SchemaName.Substring(1, this.SchemaName.Length - 2);

                // resolve the environment variable
                this.SchemaName = connectionStringResolver.Resolve(this.SchemaName)?.Value;
            }

            IConfigurationSection connectionStringSection = connectionStringResolver.Resolve(this.ConnectionStringName);
            if (connectionStringSection == null || string.IsNullOrEmpty(connectionStringSection.Value))
            {
                throw new InvalidOperationException(
                    $"No SQL connection string configuration was found for the app setting or environment variable named '{this.ConnectionStringName}'.");
            }

            // Validate the connection string
            try
            {
                new SqlConnectionStringBuilder(connectionStringSection.Value);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("The provided connection string is invalid.", e);
            }

            var settings = new SqlOrchestrationServiceSettings(connectionStringSection.Value, this.TaskHubName, this.SchemaName)
            {
                CreateDatabaseIfNotExists = this.CreateDatabaseIfNotExists,
                LoggerFactory = this.LoggerFactory,
                WorkItemBatchSize = this.TaskEventBatchSize,
                WorkItemLockTimeout = this.TaskEventLockTimeout,
            };

            if (extensionOptions.MaxConcurrentActivityFunctions.HasValue)
            {
                settings.MaxConcurrentActivities = extensionOptions.MaxConcurrentActivityFunctions.Value;
            }

            if (extensionOptions.MaxConcurrentOrchestratorFunctions.HasValue)
            {
                settings.MaxActiveOrchestrations = extensionOptions.MaxConcurrentOrchestratorFunctions.Value;
            }

            return settings;
        }
    }
}
