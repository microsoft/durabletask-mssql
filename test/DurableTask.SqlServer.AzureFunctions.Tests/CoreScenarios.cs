// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.SqlServer.Tests.Utils;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("CoreScenarios")]
    public class CoreScenarios : IntegrationTestBase
    {
        protected CoreScenarios(ITestOutputHelper output, string taskHubName, bool multiTenancy)
            : base(output, taskHubName, multiTenancy)
        {
            this.AddFunctions(typeof(Functions));
        }

        public CoreScenarios(ITestOutputHelper output)
            : base(output, "TaskHubWithMultiTenancy", true)
        {
            this.AddFunctions(typeof(Functions));
        }

        [Fact]
        public void HostCanStartAndStop()
        {
            // Ensure (via logs) that the Durable extension is loaded
            IEnumerable<string> extensionLogs = this.GetExtensionLogs();
            Assert.NotEmpty(extensionLogs);

            // Ensure (via logs) that the SQL provider correctly loaded.
            IEnumerable<string> providerLogs = this.GetProviderLogs();
            Assert.NotEmpty(providerLogs);
        }

        [Fact]
        public async Task CanRunActivitySequences()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.Sequence));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(10, (int)status.Output);
        }

        [Fact]
        public async Task CanRunFanOutFanIn()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.FanOutFanIn));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(
                expected: @"[""9"",""8"",""7"",""6"",""5"",""4"",""3"",""2"",""1"",""0""]",
                actual: status.Output?.ToString(Formatting.None));
        }

        [Fact]
        public async Task SupportsDeterministicGuid()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.DeterministicGuid));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.True((bool)status.Output);
        }

        [Fact]
        public async Task CanOrchestrateEntities()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.OrchestrateCounterEntity));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(7, (int)status.Output);
        }

        [Fact]
        public async Task CanClientInteractWithEntities()
        {
            IDurableClient client = this.GetDurableClient();

            var entityId = new EntityId(nameof(Functions.Counter), Guid.NewGuid().ToString("N"));
            EntityStateResponse<int> result = await client.ReadEntityStateAsync<int>(entityId);
            Assert.False(result.EntityExists);

            await Task.WhenAll(
                client.SignalEntityAsync(entityId, "incr"),
                client.SignalEntityAsync(entityId, "incr"),
                client.SignalEntityAsync(entityId, "incr"),
                client.SignalEntityAsync(entityId, "add", 4));

            await Task.Delay(TimeSpan.FromSeconds(5));

            result = await client.ReadEntityStateAsync<int>(entityId);
            Assert.True(result.EntityExists);
            Assert.Equal(7, result.EntityState);
        }

        [Fact]
        public async Task CanOrchestrationInteractWithEntities()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.IncrementThenGet));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(1, (int)status.Output);
        }

        [Fact]
        public async Task SingleInstanceQuery()
        {
            object input = 42;

            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.Sequence), input);
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(10, (int)status.Output);

            IDurableClient client = this.GetDurableClient();
            status = await client.GetStatusAsync(status.InstanceId);

            Assert.NotNull(status.Input);
            Assert.Equal(JTokenType.Integer, status.Input.Type);
            Assert.Equal(42, status.Input);
            Assert.Null(status.History);

            status = await client.GetStatusAsync(status.InstanceId, showHistory: true);
            Assert.NotNull(status.Input);
            Assert.Equal(JTokenType.Integer, status.Input.Type);
            Assert.Equal(42, status.Input);
            Assert.NotNull(status.History);
            Assert.NotEmpty(status.History);
            Assert.True(status.History.Count >= 12);

            // TODO: Check the content of the history for input/output fields
        }

        [Fact]
        public async Task MultiInstanceQueries()
        {
            DateTime startTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();
            string prefix = $"{startTime:HHmmss}";

            await Enumerable.Range(0, 5).ParallelForEachAsync(5, i =>
                this.RunOrchestrationAsync(
                    nameof(Functions.Sequence),
                    instanceId: $"{prefix}.sequence.{i}"));

            // As query is using CreatedTimeFrom, ensure sequencesFinishedTime is more than 100ns
            // after startTime to cope with sql server datetime2 and c# DateTime precision
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            DateTime sequencesFinishedTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            // As query is using CreatedTimeTo, ensure new orchestrations start more than 100ns
            // after sequencesFinishedTime to cope with sql server datetime2 and c# DateTime precision
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            await Enumerable.Range(0, 5).ParallelForEachAsync(5, i =>
                this.StartOrchestrationAsync(
                    nameof(Functions.WaitForEvent),
                    input: i.ToString(),
                    instanceId: $"{prefix}.waiter.{i}"));

            // As query is using CreatedTimeFrom and CreatedTimeTo, ensure afterAllFinishedTime is
            // more than 100ns after previous orchestrations start to cope with sql server datetime2
            // and c# DateTime precision
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            DateTime afterAllFinishedTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            IDurableClient client = this.GetDurableClient();

            // Create one condition object and reuse it for multiple queries
            var condition = new OrchestrationStatusQueryCondition();
            OrchestrationStatusQueryResult result;

            // Return all instances
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test CreatedTimeTo filter
            condition.CreatedTimeTo = startTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Empty(result.DurableOrchestrationState);

            condition.CreatedTimeTo = sequencesFinishedTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(5, result.DurableOrchestrationState.Count());

            condition.CreatedTimeTo = afterAllFinishedTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test CreatedTimeFrom filter
            condition.CreatedTimeFrom = afterAllFinishedTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Empty(result.DurableOrchestrationState);

            condition.CreatedTimeFrom = sequencesFinishedTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(5, result.DurableOrchestrationState.Count());

            condition.CreatedTimeFrom = startTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test RuntimeStatus filter
            var statusFilters = new HashSet<OrchestrationRuntimeStatus>(new[]
            {
                OrchestrationRuntimeStatus.Failed,
                OrchestrationRuntimeStatus.Pending,
                OrchestrationRuntimeStatus.Terminated
            });

            condition.RuntimeStatus = statusFilters;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Empty(result.DurableOrchestrationState);

            statusFilters.Add(OrchestrationRuntimeStatus.Running);
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(5, result.DurableOrchestrationState.Count());
            Assert.All(result.DurableOrchestrationState, state => Assert.Equal(OrchestrationRuntimeStatus.Running, state.RuntimeStatus));

            statusFilters.Add(OrchestrationRuntimeStatus.Completed);
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            statusFilters.Remove(OrchestrationRuntimeStatus.Running);
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(5, result.DurableOrchestrationState.Count());
            Assert.All(result.DurableOrchestrationState, state => Assert.Equal(OrchestrationRuntimeStatus.Completed, state.RuntimeStatus));

            statusFilters.Clear();
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test InstanceIdPrefix
            condition.InstanceIdPrefix = "Foo";
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Empty(result.DurableOrchestrationState);

            condition.InstanceIdPrefix = prefix;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test PageSize and ContinuationToken
            var instanceIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            condition.PageSize = 0;
            while (condition.PageSize++ < 10)
            {
                result = await client.ListInstancesAsync(condition, CancellationToken.None);
                int total = result.DurableOrchestrationState.Count();
                Assert.Equal(condition.PageSize, total);

                // Make sure all instance IDs are unique
                Assert.All(result.DurableOrchestrationState, s => instanceIds.Add(s.InstanceId));

                while (total < 10)
                {
                    condition.ContinuationToken = result.ContinuationToken;
                    result = await client.ListInstancesAsync(condition, CancellationToken.None);
                    int count = result.DurableOrchestrationState.Count();
                    Assert.NotEqual(0, count);
                    Assert.True(count <= condition.PageSize);
                    total += count;
                    Assert.True(total <= 10);
                    Assert.All(result.DurableOrchestrationState, s => instanceIds.Add(s.InstanceId));
                }

                condition.ContinuationToken = null;
            }

            // Test ShowInput
            condition.ShowInput = true;
            condition.CreatedTimeFrom = sequencesFinishedTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.All(result.DurableOrchestrationState, state => Assert.NotEqual(JValue.CreateNull(), state.Input));

            condition.ShowInput = false;
            condition.CreatedTimeFrom = startTime;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.All(result.DurableOrchestrationState, state => Assert.Equal(JValue.CreateNull(), state.Input));
        }


        [Fact]
        public async Task SingleInstancePurge()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.NoOp));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);

            IDurableClient client = this.GetDurableClient();
            PurgeHistoryResult result;

            // First purge gets the active instance
            result = await client.PurgeInstanceHistoryAsync(status.InstanceId);
            Assert.NotNull(result);
            Assert.Equal(1, result.InstancesDeleted);

            // Verify that it's gone
            DurableOrchestrationStatus statusAfterPurge = await client.GetStatusAsync(status.InstanceId);
            Assert.Null(statusAfterPurge);

            // Second purge should be a no-op
            result = await client.PurgeInstanceHistoryAsync(status.InstanceId);
            Assert.NotNull(result);
            Assert.Equal(0, result.InstancesDeleted);
        }

        [Fact]
        public async Task MultiInstancePurge()
        {
            DateTime startTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            DurableOrchestrationStatus instance1 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));
            DurableOrchestrationStatus instance2 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));

            // As purge is using CreatedTimeFrom, ensure new orchestrations are started more than 100ns
            // after startTime to cope with sql server datetime2 and c# DateTime precision
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            DateTime midTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            DurableOrchestrationStatus instance3 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));
            DurableOrchestrationStatus instance4 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));

            // As purge is using CreatedTimeFrom, ensure endTime is more than 100ns after midTime
            // to cope with sql server datetime2 and c# DateTime precision
            await Task.Delay(TimeSpan.FromMilliseconds(20));

            DateTime endTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            IDurableClient client = this.GetDurableClient();
            PurgeHistoryResult purgeResult;

            // First attempt should delete nothing because it's out of range
            purgeResult = await client.PurgeInstanceHistoryAsync(endTime, null, null);
            Assert.NotNull(purgeResult);
            Assert.Equal(0, purgeResult.InstancesDeleted);

            // Purging from the test's mid-point should purge instance3 and instance4
            purgeResult = await client.PurgeInstanceHistoryAsync(midTime, null, null);
            Assert.NotNull(purgeResult);
            Assert.Equal(2, purgeResult.InstancesDeleted);
            Assert.NotNull(await client.GetStatusAsync(instance1.InstanceId));
            Assert.NotNull(await client.GetStatusAsync(instance2.InstanceId));
            Assert.Null(await client.GetStatusAsync(instance3.InstanceId));
            Assert.Null(await client.GetStatusAsync(instance4.InstanceId));

            // Purging from the test start time should purge instance1 and instance2
            purgeResult = await client.PurgeInstanceHistoryAsync(startTime, null, null);
            Assert.Equal(2, purgeResult.InstancesDeleted);
            Assert.Null(await client.GetStatusAsync(instance1.InstanceId));
            Assert.Null(await client.GetStatusAsync(instance2.InstanceId));

            // Purging again should be a no-op
            purgeResult = await client.PurgeInstanceHistoryAsync(startTime, null, null);
            Assert.Equal(0, purgeResult.InstancesDeleted);
        }

        [Fact]
        public async Task CanInvokeSubOrchestration()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.SubOrchestrationTest));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal("done", status.Output);
        }

        [Fact]
        public async Task CanRewindOrchestration()
        {
            Functions.ThrowExceptionInCanFail = true;
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.RewindOrchestration));
            Assert.Equal(OrchestrationRuntimeStatus.Failed, status.RuntimeStatus);

            Functions.ThrowExceptionInCanFail = false;
            status = await this.RewindOrchestrationAsync(status.InstanceId);
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal("activity", status.Output);
        }

        [Fact]
        public async Task CanRewindSubOrchestration()
        {
            Functions.ThrowExceptionInCanFail = true;
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.RewindSubOrchestration));
            Assert.Equal(OrchestrationRuntimeStatus.Failed, status.RuntimeStatus);

            Functions.ThrowExceptionInCanFail = false;
            status = await this.RewindOrchestrationAsync(status.InstanceId);
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal("0,1,2,activity", status.Output);
        }
    }
}
