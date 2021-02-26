// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Newtonsoft.Json;

    public class SqlDurabilityProviderFactory : IDurabilityProviderFactory
    {
        readonly Dictionary<string, DurabilityProvider> clientProviders =
            new Dictionary<string, DurabilityProvider>(StringComparer.OrdinalIgnoreCase);

        readonly DurableTaskOptions extensionOptions;
        readonly ILoggerFactory loggerFactory;
        readonly IConnectionStringResolver connectionStringResolver;

        SqlDurabilityOptions? defaultOptions;
        SqlOrchestrationService? service;
        SqlDurabilityProvider? defaultProvider;

        // Called by the Azure Functions runtime dependency injection infrastructure
        public SqlDurabilityProviderFactory(
            IOptions<DurableTaskOptions> extensionOptions,
            ILoggerFactory loggerFactory,
            IConnectionStringResolver connectionStringResolver)
        {
            this.extensionOptions = extensionOptions?.Value ?? throw new ArgumentNullException(nameof(extensionOptions));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.connectionStringResolver = connectionStringResolver ?? throw new ArgumentNullException(nameof(connectionStringResolver));
        }

        // Called by the Durable trigger binding infrastructure
        public DurabilityProvider GetDurabilityProvider()
        {
            if (this.defaultProvider == null)
            {
                SqlDurabilityOptions options = this.GetDefaultSqlOptions();
                SqlOrchestrationService service = this.GetOrchestrationService();
                this.defaultProvider = new SqlDurabilityProvider(service, options);
            }

            return this.defaultProvider;
        }

        // Called by the Durable client binding infrastructure
        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            // TODO: Much of this logic should go into the base class
            if (string.IsNullOrEmpty(attribute.ConnectionName) &&
                string.IsNullOrEmpty(attribute.TaskHub))
            {
                return this.GetDurabilityProvider();
            }

            lock (this.clientProviders)
            {
                string key = GetDurabilityProviderKey(attribute);
                if (this.clientProviders.TryGetValue(key, out DurabilityProvider clientProvider))
                {
                    return clientProvider;
                }

                SqlDurabilityOptions clientOptions = this.GetSqlOptions(attribute);
                IOrchestrationServiceClient serviceClient = 
                    new SqlOrchestrationService(clientOptions.ProviderOptions);
                clientProvider = new SqlDurabilityProvider(
                    this.GetOrchestrationService(),
                    clientOptions,
                    serviceClient);

                this.clientProviders.Add(key, clientProvider);
                return clientProvider;
            }
        }

        static string GetDurabilityProviderKey(DurableClientAttribute attribute)
        {
            return attribute.ConnectionName + "|" + attribute.TaskHub;
        }

        SqlOrchestrationService GetOrchestrationService()
        {
            if (this.service == null)
            {
                this.service = new SqlOrchestrationService(this.GetDefaultSqlOptions().ProviderOptions);
            }

            return this.service;
        }

        SqlDurabilityOptions GetDefaultSqlOptions()
        {
            if (this.defaultOptions == null)
            {
                this.defaultOptions = this.GetSqlOptions(new DurableClientAttribute());
            }

            return this.defaultOptions;
        }

        SqlDurabilityOptions GetSqlOptions(DurableClientAttribute attribute)
        {
            var options = new SqlDurabilityOptions();

            // Deserialize the configuration directly from the host.json settings.
            // Note that not all settings can be applied from JSON.
            string configJson = JsonConvert.SerializeObject(this.extensionOptions.StorageProvider);
            JsonConvert.PopulateObject(configJson, options);

            string connectionStringName = attribute.ConnectionName ?? options.ConnectionStringName;
            string? connectionString = this.connectionStringResolver.Resolve(connectionStringName);
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException(
                    $"No SQL connection string configuration was found for the app setting or environment variable named '{connectionStringName}'.");
            }

            // Validate the connection string
            try
            {
                new SqlConnectionStringBuilder(connectionString);
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException("The provided connection string is invalid.", e);
            }

            SqlProviderOptions providerOptions = options.ProviderOptions;
            providerOptions.ConnectionString = connectionString;
            providerOptions.LoggerFactory = this.loggerFactory;
            providerOptions.WorkItemLockTimeout = options.TaskEventLockTimeout;
            return options;
        }
    }
}
