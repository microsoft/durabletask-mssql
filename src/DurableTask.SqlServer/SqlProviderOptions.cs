// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Newtonsoft.Json;

    public class SqlProviderOptions
    {
        [JsonProperty("workItemBatchSize")]
        public int WorkItemBatchSize { get; set; } = 10;

        [JsonProperty("workItemLockTimeout")]
        public TimeSpan WorkItemLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        [JsonProperty("appName")]
        public string AppName { get; set; } = Environment.MachineName;

        // Not serializeable (security sensitive) - must be initializd in code
        public string ConnectionString { get; set; } = GetDefaultConnectionString();

        // Not serializeable (complex object) - must be initialized in code
        public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

        internal SqlConnection CreateConnection() => new SqlConnection(this.ConnectionString);

        internal static string GetDefaultConnectionString()
        {
            // The default for local development on a Windows OS
            string defaultConnectionString = "Server=localhost;Database=DurableDB;Trusted_Connection=True;";

            // The use of SA_PASSWORD is intended for use with the mssql docker container
            string saPassword = Environment.GetEnvironmentVariable("SA_PASSWORD");
            if (string.IsNullOrEmpty(saPassword))
            {
                return defaultConnectionString;
            }

            var builder = new SqlConnectionStringBuilder(defaultConnectionString)
            {
                IntegratedSecurity = false,
                UserID = "sa",
                Password = saPassword,
            };

            return builder.ToString();
        }
    }
}
