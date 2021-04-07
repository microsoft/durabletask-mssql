// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        public SqlOrchestrationServiceSettings(string connectionString, string? taskHubName = null)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            this.TaskHubName = taskHubName ?? "default";

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                // We use the task hub name as the application name so that
                // stored procedures have easy access to this information.
                ApplicationName = this.TaskHubName,
            };

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
        /// Gets a SQL connection string associated with the configured task hub.
        /// </summary>
        [JsonIgnore]
        public string TaskHubConnectionString { get; }

        /// <summary>
        /// Gets a <see cref="ILoggerFactory"/> used for writing logs to the DurableTask.SqlServer trace source.
        /// </summary>
        [JsonIgnore]
        public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

        internal SqlConnection CreateConnection()
        {
            return new SqlConnection(this.TaskHubConnectionString);
        }
    }
}
