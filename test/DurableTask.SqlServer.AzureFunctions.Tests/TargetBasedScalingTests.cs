// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Microsoft.Extensions.Logging;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;

    public class TargetBasedScalingTests
    {
        readonly Mock<SqlMetricsProvider> metricsProviderMock;
        readonly Mock<IOrchestrationService> orchestrationServiceMock;

        public TargetBasedScalingTests()
        {
            this.orchestrationServiceMock = new Mock<IOrchestrationService>(MockBehavior.Strict);

            this.metricsProviderMock = new Mock<SqlMetricsProvider>(
                MockBehavior.Strict,
                null,
                null);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(20)]
        public async void TargetBasedScalingTest(int expectedTargetWorkerCount)
        {
            var durabilityProviderMock = new Mock<DurabilityProvider>(
                MockBehavior.Strict,
                "storageProviderName",
                this.orchestrationServiceMock.Object,
                new Mock<IOrchestrationServiceClient>().Object,
                "connectionName");

            SqlScaleMetric sqlScaleMetric = new SqlScaleMetric()
            {
                RecommendedReplicaCount = expectedTargetWorkerCount,
            };

            this.metricsProviderMock.Setup(m => m.GetMetricsAsync(null)).ReturnsAsync(sqlScaleMetric);

            SqlTargetScaler targetScaler = new SqlTargetScaler(
                "functionId",
                this.metricsProviderMock.Object);

            TargetScalerResult result = await targetScaler.GetScaleResultAsync(new TargetScalerContext());

            Assert.Equal(expectedTargetWorkerCount, result.TargetWorkerCount);
        }
    }
}
