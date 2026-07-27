// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Xunit;

    public class QueueModeTests
    {
        [Fact]
        public void CanSwitchEntityQueueModes()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddOptions();
            services.Configure<DurableTaskOptions>(options => options.HubName = "QueueModeTests");
            services.AddSingleton<IConnectionInfoResolver>(
                new TestConnectionInfoResolver("Server=(local);Database=DurableTask;Integrated Security=true"));
            services.AddDurableTaskSqlProvider();

            using ServiceProvider serviceProvider = services.BuildServiceProvider();
            IDurabilityProviderFactory factory = serviceProvider.GetRequiredService<IDurabilityProviderFactory>();

            factory.SetUseSeparateQueueForEntityWorkItems(true);
            DurabilityProvider provider = factory.GetDurabilityProvider();
            provider.SetUseSeparateQueueForEntityWorkItems(true);

            factory.SetUseSeparateQueueForEntityWorkItems(false);
            provider.SetUseSeparateQueueForEntityWorkItems(false);
        }

        class TestConnectionInfoResolver : IConnectionInfoResolver
        {
            readonly IConfiguration configuration;

            public TestConnectionInfoResolver(string connectionString)
            {
                this.configuration = new ConfigurationBuilder()
                    .AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["SQLDB_Connection"] = connectionString,
                    })
                    .Build();
            }

            public IConfigurationSection Resolve(string name) => this.configuration.GetSection(name);
        }
    }
}
