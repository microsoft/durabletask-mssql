// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Newtonsoft.Json;

    /// <summary>
    /// Microsoft SQL <see cref="IDurabilityProviderFactory"/> implementation for Durable Tasks in Azure Functions.
    /// </summary>
    class SqlDurabilityProviderFactory : IDurabilityProviderFactory
    {
        readonly Dictionary<string, DurabilityProvider> clientProviders =
            new Dictionary<string, DurabilityProvider>(StringComparer.OrdinalIgnoreCase);

        readonly DurableTaskOptions extensionOptions;
        readonly ILoggerFactory loggerFactory;
        readonly IConnectionStringResolver connectionStringResolver;

        SqlDurabilityOptions? defaultOptions;
        SqlOrchestrationServiceSettings? orchestrationServiceSettings;
        SqlOrchestrationService? service;
        SqlDurabilityProvider? defaultProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlDurabilityProviderFactory"/> class.
        /// </summary>
        /// <remarks>
        /// Intended to be called by the Azure Functions runtime dependency injection infrastructure.
        /// </remarks>
        /// <param name="extensionOptions">Durable task extension configuration options.</param>
        /// <param name="loggerFactory">Logger factory registered with the Azure Functions runtime.</param>
        /// <param name="connectionStringResolver">Resolver service for fetching Durable Task connection string information.</param>
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
        public string Name => SqlDurabilityProvider.Name;

        // Called by the Durable trigger binding infrastructure
        public DurabilityProvider GetDurabilityProvider()
        {
            if (this.defaultProvider == null)
            {
                SqlDurabilityOptions sqlProviderOptions = this.GetDefaultSqlOptions();
                SqlOrchestrationService service = this.GetOrchestrationService();
                this.defaultProvider = new SqlDurabilityProvider(service, sqlProviderOptions);
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
                    new SqlOrchestrationService(clientOptions.GetOrchestrationServiceSettings(
                        this.extensionOptions,
                        this.connectionStringResolver));
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
                SqlOrchestrationServiceSettings settings = this.GetOrchestrationServiceSettings();
                this.service = new SqlOrchestrationService(settings);
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
            var options = new SqlDurabilityOptions
            {
                TaskHubName = this.extensionOptions.HubName,
                LoggerFactory = this.loggerFactory,
            };

            // Deserialize the configuration directly from the host.json settings.
            // Note that not all settings can be applied from JSON.
            string configJson = JsonConvert.SerializeObject(this.extensionOptions.StorageProvider);
            JsonConvert.PopulateObject(configJson, options);

            // Attribute properties can override host.json settings.
            if (!string.IsNullOrEmpty(attribute.ConnectionName))
            {
                options.ConnectionStringName = attribute.ConnectionName;
            }

            if (!string.IsNullOrEmpty(attribute.TaskHub))
            {
                options.TaskHubName = attribute.TaskHub;
            }

            return options;
        }

        SqlOrchestrationServiceSettings GetOrchestrationServiceSettings()
        {
            if (this.orchestrationServiceSettings == null)
            {
                SqlDurabilityOptions options = this.GetDefaultSqlOptions();
                this.orchestrationServiceSettings = options.GetOrchestrationServiceSettings(
                    this.extensionOptions,
                    this.connectionStringResolver);
            }

            return this.orchestrationServiceSettings;
        }
    }
}
