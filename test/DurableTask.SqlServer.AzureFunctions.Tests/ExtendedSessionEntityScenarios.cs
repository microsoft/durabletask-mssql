// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Runs the Durable Entity scenarios end-to-end with extended sessions enabled, so that both the
    /// calling orchestrations and the entities process several turns within a single session.
    /// </summary>
    [Collection("Integration")]
    public class ExtendedSessionEntityScenarios : IntegrationTestBase
    {
        public ExtendedSessionEntityScenarios(ITestOutputHelper output)
            : base(output, "TaskHubWithExtendedSessions", multiTenancy: true, extendedSessions: true)
        {
            this.AddFunctions(typeof(Functions));
        }

        [Fact]
        public async Task CanOrchestrateEntities()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.OrchestrateCounterEntity));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(7, (int)status.Output);
        }

        [Fact]
        public async Task CanOrchestrationInteractWithEntities()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.IncrementThenGet));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(1, (int)status.Output);
        }

        [Fact]
        public async Task CanClientInteractWithEntities()
        {
            IDurableClient client = this.GetDurableClient();

            var entityId = new EntityId(nameof(Functions.Counter), Guid.NewGuid().ToString("N"));
            EntityStateResponse<int> result = await client.ReadEntityStateAsync<int>(entityId);
            Assert.False(result.EntityExists);

            // Signal one at a time so the entity receives them across several turns of its session.
            await client.SignalEntityAsync(entityId, "incr");
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await client.SignalEntityAsync(entityId, "incr");
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await client.SignalEntityAsync(entityId, "incr");
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await client.SignalEntityAsync(entityId, "add", 4);

            await Task.Delay(TimeSpan.FromSeconds(5));

            result = await client.ReadEntityStateAsync<int>(entityId);
            Assert.True(result.EntityExists);
            Assert.Equal(7, result.EntityState);
        }
    }
}
