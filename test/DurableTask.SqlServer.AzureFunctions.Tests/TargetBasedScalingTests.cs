// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Moq;
    using Xunit;

    public class TargetBasedScalingTests
    {
        readonly Mock<SqlMetricsProvider> metricsProviderMock;
        readonly Mock<IOrchestrationService> orchestrationServiceMock;

        public TargetBasedScalingTests()
        {
            this.orchestrationServiceMock = new Mock<IOrchestrationService>(MockBehavior.Strict);

            SqlOrchestrationService? nullServiceArg = null; // not needed for this test
            this.metricsProviderMock = new Mock<SqlMetricsProvider>(
                behavior: MockBehavior.Strict,
                nullServiceArg!);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(20)]
        public async Task TargetBasedScalingTest(int expectedTargetWorkerCount)
        {
            var durabilityProviderMock = new Mock<DurabilityProvider>(
                MockBehavior.Strict,
                "storageProviderName",
                this.orchestrationServiceMock.Object,
                new Mock<IOrchestrationServiceClient>().Object,
                "connectionName");

            var sqlScaleMetric = new SqlScaleMetric()
            {
                RecommendedReplicaCount = expectedTargetWorkerCount,
            };

            this.metricsProviderMock.Setup(m => m.GetMetricsAsync(null)).ReturnsAsync(sqlScaleMetric);

            var targetScaler = new SqlTargetScaler(
                "functionId",
                this.metricsProviderMock.Object);

            TargetScalerResult result = await targetScaler.GetScaleResultAsync(new TargetScalerContext());

            Assert.Equal(expectedTargetWorkerCount, result.TargetWorkerCount);
        }
    }
}
