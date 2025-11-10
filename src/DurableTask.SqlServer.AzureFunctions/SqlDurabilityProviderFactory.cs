// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
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
        readonly IConnectionInfoResolver connectionInfoResolver;

        readonly bool useSeparateQueueForEntityWorkItems = false;

        SqlDurabilityOptions? defaultOptions;
        SqlDurabilityProvider? defaultProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlDurabilityProviderFactory"/> class.
        /// </summary>
        /// <remarks>
        /// Intended to be called by the Azure Functions runtime dependency injection infrastructure.
        /// </remarks>
        /// <param name="extensionOptions">Durable task extension configuration options.</param>
        /// <param name="loggerFactory">Logger factory registered with the Azure Functions runtime.</param>
        /// <param name="connectionInfoResolver">Resolver service for fetching Durable Task connection string information.</param>
        public SqlDurabilityProviderFactory(
            IOptions<DurableTaskOptions> extensionOptions,
            ILoggerFactory loggerFactory,
            IConnectionInfoResolver connectionInfoResolver,
#pragma warning disable CS0612 // Type or member is obsolete
            IPlatformInformation platformInfo)
#pragma warning restore CS0612 // Type or member is obsolete
        {
            this.extensionOptions = extensionOptions?.Value ?? throw new ArgumentNullException(nameof(extensionOptions));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.connectionInfoResolver = connectionInfoResolver ?? throw new ArgumentNullException(nameof(connectionInfoResolver));

            WorkerRuntimeType runtimeType = platformInfo.GetWorkerRuntimeType();
            if (runtimeType == WorkerRuntimeType.DotNetIsolated ||
                runtimeType == WorkerRuntimeType.Java ||
                runtimeType == WorkerRuntimeType.Custom)
            {
                this.useSeparateQueueForEntityWorkItems = true;
            }
        }

        // Called by the Durable trigger binding infrastructure
        public string Name => SqlDurabilityProvider.Name;

        // Called by the Durable trigger binding infrastructure
        public DurabilityProvider GetDurabilityProvider()
        {
            if (this.defaultProvider == null)
            {
                SqlDurabilityOptions sqlProviderOptions = this.GetDefaultSqlOptions();
                SqlOrchestrationService service = this.GetOrchestrationService(sqlProviderOptions);
                this.defaultProvider = new SqlDurabilityProvider(service, sqlProviderOptions);
            }

            return this.defaultProvider;
        }

        // Called by the Durable client binding infrastructure
        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            lock (this.clientProviders)
            {
                string key = GetDurabilityProviderKey(attribute);
                if (this.clientProviders.TryGetValue(key, out DurabilityProvider? clientProvider))
                {
                    return clientProvider;
                }

                SqlDurabilityOptions clientOptions = this.GetSqlOptions(attribute);
                SqlOrchestrationService orchestrationService = 
                    this.GetOrchestrationService(clientOptions);
                clientProvider = new SqlDurabilityProvider(
                    orchestrationService,
                    clientOptions);

                this.clientProviders.Add(key, clientProvider);
                return clientProvider;
            }
        }

        SqlOrchestrationService GetOrchestrationService(SqlDurabilityOptions clientOptions)
        {
            SqlOrchestrationServiceSettings sqlOrchestrationServiceSettings = clientOptions.GetOrchestrationServiceSettings(
                this.extensionOptions,
                this.connectionInfoResolver);
            sqlOrchestrationServiceSettings.UseSeparateQueueForEntityWorkItems = this.useSeparateQueueForEntityWorkItems;
            return new(sqlOrchestrationServiceSettings);
        }

        static string GetDurabilityProviderKey(DurableClientAttribute attribute)
        {
            return attribute.ConnectionName + "|" + attribute.TaskHub;
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
    }
}
