// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Newtonsoft.Json;

    /// <summary>
    /// Configuration settings for the <see cref="SqlOrchestrationService"/>.
    /// </summary>
    public class SqlOrchestrationServiceSettings
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlOrchestrationServiceSettings"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string for connecting to the database.</param>
        /// <param name="taskHubName">Optional. The name of the task hub. If not specified, a default name will be used.</param>
        /// <param name="schemaName">Optional. The name of the schema. If not specified, the default 'dt' value will be used.</param>
        public SqlOrchestrationServiceSettings(string connectionString, string? taskHubName = null, string? schemaName = null)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            this.TaskHubName = taskHubName ?? "default";
            this.SchemaName = schemaName ?? "dt";

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                // We use the task hub name as the application name so that
                // stored procedures have easy access to this information.
                ApplicationName = this.TaskHubName,
            };

            if (string.IsNullOrEmpty(builder.InitialCatalog))
            {
                throw new ArgumentException("Database or Initial Catalog must be specified in the connection string.", nameof(connectionString));
            }

            this.DatabaseName = builder.InitialCatalog;
            this.TaskHubConnectionString = builder.ToString();
        }

        /// <summary>
        /// Gets or sets the number of events that can be dequeued at a time.
        /// </summary>
        [JsonProperty("workItemBatchSize")]
        public int WorkItemBatchSize { get; set; } = 10;

        /// <summary>
        /// Gets or sets the amount of time a work item is locked after being dequeued.
        /// </summary>
        [JsonProperty("workItemLockTimeout")]
        public TimeSpan WorkItemLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// Gets or sets the name of the task hub.
        /// </summary>
        [JsonProperty("taskHubName")]
        public string TaskHubName { get; }
        
        /// <summary>
        /// Gets the name of the schema.
        /// </summary>
        [JsonProperty("schemaName")]
        public string SchemaName { get; }

        /// <summary>
        /// Gets or sets the name of the app. Used for logging purposes.
        /// </summary>
        [JsonProperty("appName")]
        public string AppName { get; set; } = Environment.MachineName;

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently by a single worker.
        /// The default value is the value of <see cref="Environment.ProcessorCount"/>.
        /// </summary>
        [JsonProperty("maxConcurrentActivities")]
        public int MaxConcurrentActivities { get; set; } = Environment.ProcessorCount;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be loaded in memory at a time by a single worker.
        /// The default value is the value of <see cref="Environment.ProcessorCount"/>.
        /// </summary>
        /// <remarks>
        /// Orchestrations that are idle and waiting for inputs are unloaded from memory and do not count against this limit.
        /// </remarks>
        [JsonProperty("maxActiveOrchestrations")]
        public int MaxActiveOrchestrations { get; set; } = Environment.ProcessorCount;

        /// <summary>
        /// Gets or sets the maximum interval to poll for orchestrations.
        /// Polling interval increases when no orchestrations or activities are found.
        /// The default value is 3 seconds.
        /// </summary>
        [JsonProperty("maxOrchestrationPollingInterval")]
        public TimeSpan MaxOrchestrationPollingInterval { get; set; } = TimeSpan.FromSeconds(3);

        /// <summary>
        /// Gets or sets the maximum interval to poll for activities.
        /// Polling interval increases when no activities are found.
        /// The default value is 3 seconds.
        /// </summary>
        [JsonProperty("maxActivityPollingInterval")]
        public TimeSpan MaxActivityPollingInterval { get; set; } = TimeSpan.FromSeconds(3);

        /// <summary>
        /// Gets or sets a flag indicating whether the database should be automatically created if it does not exist.
        /// </summary>
        /// <remarks>
        /// If <see langword="true"/>, the user requires the permission <c>CREATE DATABASE</c>.
        /// </remarks>
        [JsonProperty("createDatabaseIfNotExists")]
        public bool CreateDatabaseIfNotExists { get; set; }

        /// <summary>
        /// Gets a SQL connection string associated with the configured task hub.
        /// </summary>
        [JsonIgnore]
        public string TaskHubConnectionString { get; }

        /// <summary>
        /// Gets a <see cref="ILoggerFactory"/> used for writing logs to the DurableTask.SqlServer trace source.
        /// </summary>
        [JsonIgnore]
        public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

        /// <summary>
        /// Gets or sets the name of the database that contains the instance store.
        /// </summary>
        /// <remarks>
        /// This value is derived from the value of the <c>"Initial Catalog"</c> or <c>"Database"</c>
        /// attribute in the <see cref="TaskHubConnectionString"/>.
        /// </remarks>
        [JsonIgnore]
        public string DatabaseName { get; set; }

        internal SqlConnection CreateConnection() => new SqlConnection(this.TaskHubConnectionString);

        internal SqlConnection CreateConnection(string databaseName)
        {
            if (databaseName == this.DatabaseName)
            {
                return this.CreateConnection();
            }

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(this.TaskHubConnectionString)
            {
                InitialCatalog = databaseName
            };

            return new SqlConnection(builder.ToString());
        }
    }
}
