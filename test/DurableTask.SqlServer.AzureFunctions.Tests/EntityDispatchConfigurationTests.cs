// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System.Collections.Generic;
    using DurableTask.Core.Entities;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.Extensions.Options;
    using Moq;
    using Xunit;

    public class EntityDispatchConfigurationTests
    {
        [Fact]
        public void SetUseSeparateQueueForEntityWorkItemsUpdatesBackendProperties()
        {
            var settings = new SqlOrchestrationServiceSettings(
                "Server=(local);Database=TestDatabase;Integrated Security=true");
            var service = new SqlOrchestrationService(settings);
            var provider = new SqlDurabilityProvider(service, new SqlDurabilityOptions());

            provider.SetUseSeparateQueueForEntityWorkItems(true);

            Assert.True(service.EntityBackendProperties.UseSeparateQueueForEntityWorkItems);
        }

        [Fact]
        public void FactoryAppliesSeparateQueueSettingToNewProvider()
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new[]
                {
                    new KeyValuePair<string, string?>("SQLDB_Connection", "Server=(local);Database=TestDatabase;Integrated Security=true"),
                })
                .Build();
            var connectionInfoResolver = new Mock<IConnectionInfoResolver>();
            connectionInfoResolver
                .Setup(resolver => resolver.Resolve("SQLDB_Connection"))
                .Returns(configuration.GetSection("SQLDB_Connection"));
#pragma warning disable CS0612 // IPlatformInformation is obsolete but required by the provider factory constructor.
            var platformInformation = new Mock<IPlatformInformation>();
            platformInformation.Setup(platform => platform.GetWorkerRuntimeType()).Returns(WorkerRuntimeType.DotNet);
#pragma warning restore CS0612
            var factory = new SqlDurabilityProviderFactory(
                Options.Create(new DurableTaskOptions()),
                NullLoggerFactory.Instance,
                connectionInfoResolver.Object,
                platformInformation.Object);

            factory.SetUseSeparateQueueForEntityWorkItems(true);
            var provider = (SqlDurabilityProvider)factory.GetDurabilityProvider();
            EntityBackendProperties? backendProperties = ((IEntityOrchestrationService)provider).EntityBackendProperties;

            Assert.NotNull(backendProperties);
            Assert.True(backendProperties!.UseSeparateQueueForEntityWorkItems);
        }
    }
}