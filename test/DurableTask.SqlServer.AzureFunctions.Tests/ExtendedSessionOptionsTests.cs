// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Configuration;
    using Moq;
    using Xunit;

    public class ExtendedSessionOptionsTests
    {
        [Theory]
        [InlineData(60, 60)]    // custom positive value flows through unchanged
        [InlineData(0, 0)]      // explicit zero is honored (not silently replaced by the default)
        [InlineData(-5, 0)]     // negative values are clamped to zero, matching Azure Storage
        public void MapsExtendedSessionIdleTimeout(int configuredSeconds, int expectedSeconds)
        {
            var extensionOptions = new DurableTaskOptions
            {
                ExtendedSessionIdleTimeoutInSeconds = configuredSeconds,
            };

            SqlOrchestrationServiceSettings settings = GetSettings(extensionOptions);

            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), settings.ExtendedSessionIdleTimeout);
        }

        [Fact]
        public void UnconfiguredTimeoutMapsFrameworkDefault()
        {
            var extensionOptions = new DurableTaskOptions();
            int frameworkDefault = extensionOptions.ExtendedSessionIdleTimeoutInSeconds;

            SqlOrchestrationServiceSettings settings = GetSettings(extensionOptions);

            Assert.True(frameworkDefault > 0, "Expected a positive framework default for the unconfigured case.");
        }

        static SqlOrchestrationServiceSettings GetSettings(DurableTaskOptions extensionOptions)
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

            return new SqlDurabilityOptions().GetOrchestrationServiceSettings(
                extensionOptions,
                connectionInfoResolver.Object);
        }
    }
}
