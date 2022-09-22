// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Xunit;
    using Xunit.Abstractions;

    public class WithoutMultiTenancyCoreScenarios : CoreScenarios
    {
        public WithoutMultiTenancyCoreScenarios(ITestOutputHelper output)
            : base(output, false)
        {
        }

        [Fact]
        public async Task When_Without_MultiTenancy_should_Return_Correct_Orchestration_By_TaskHub()
        {
            string otherTaskHubName = "SomeOtherTaskHub";

            string currentTaskHubInstanceId = Guid.NewGuid().ToString();
            await this.StartOrchestrationWithoutWaitingAsync(nameof(Functions.Sequence), instanceId: currentTaskHubInstanceId);

            string anotherTaskHubInstanceId = Guid.NewGuid().ToString();
            await this.StartOrchestrationWithoutWaitingAsync(nameof(Functions.Sequence), instanceId: anotherTaskHubInstanceId, taskHub: otherTaskHubName);

            IDurableClient client = this.GetDurableClient();
            var current = await client.GetStatusAsync(currentTaskHubInstanceId);
            Assert.NotNull(current);
            var otherInstance = await client.GetStatusAsync(anotherTaskHubInstanceId);
            Assert.Null(otherInstance);

            IDurableClient clientOtherTaskHub = this.GetDurableClient(otherTaskHubName);
            var currentFromOtherTaskHub = await clientOtherTaskHub.GetStatusAsync(currentTaskHubInstanceId);
            Assert.Null(currentFromOtherTaskHub);
            var otherInstanceFromOtherTaskHub = await clientOtherTaskHub.GetStatusAsync(anotherTaskHubInstanceId);
            Assert.NotNull(otherInstanceFromOtherTaskHub);
        }
    }
}