// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.SqlServer.Tests.Utils;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    public class CoreScenarios : IntegrationTestBase
    {
        public CoreScenarios(ITestOutputHelper output)
            : base(output)
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
        public async Task CanInteractWithEntities()
        {
            IDurableClient client = await this.GetDurableClientAsync();

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
        public async Task SingleInstanceQuery()
        {
            object input = 42;

            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.Sequence), input);
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(10, (int)status.Output);

            IDurableClient client = await this.GetDurableClientAsync();
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
            DateTime startTime = DateTime.UtcNow;
            string prefix = $"{startTime:HHmmss}";

            await Enumerable.Range(0, 5).ParallelForEachAsync(5, i =>
                this.RunOrchestrationAsync(
                    nameof(Functions.Sequence),
                    instanceId: $"{prefix}.sequence.{i}"));

            // Extra delay to account for test flakiness in the GitHub CI (TODO: Why is this necessary?)
            await Task.Delay(TimeSpan.FromSeconds(1));

            DateTime sequencesFinishedTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            await Enumerable.Range(0, 5).ParallelForEachAsync(5, i =>
                this.StartOrchestrationAsync(
                    nameof(Functions.WaitForEvent),
                    input: i.ToString(),
                    instanceId: $"{prefix}.waiter.{i}"));

            IDurableClient client = await this.GetDurableClientAsync();

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

            condition.CreatedTimeTo = DateTime.UtcNow;
            result = await client.ListInstancesAsync(condition, CancellationToken.None);
            Assert.Equal(10, result.DurableOrchestrationState.Count());

            // Test CreatedTimeFrom filter
            condition.CreatedTimeFrom = DateTime.UtcNow;
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

            IDurableClient client = await this.GetDurableClientAsync();
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

            // Extra delay to account for test flakiness in the GitHub CI (TODO: Why is this necessary?)
            await Task.Delay(TimeSpan.FromSeconds(1));

            DateTime midTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            DurableOrchestrationStatus instance3 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));
            DurableOrchestrationStatus instance4 = await this.RunOrchestrationAsync(nameof(Functions.NoOp));

            // Extra delay to account for test flakiness in the GitHub CI (TODO: Why is this necessary?)
            await Task.Delay(TimeSpan.FromSeconds(1));

            DateTime endTime = SharedTestHelpers.GetCurrentDatabaseTimeUtc();

            IDurableClient client = await this.GetDurableClientAsync();
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

        static class Functions
        {
            [FunctionName(nameof(Sequence))]
            public static async Task<int> Sequence(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                int value = 0;
                for (int i = 0; i < 10; i++)
                {
                    value = await ctx.CallActivityAsync<int>(nameof(PlusOne), value);
                }

                return value;
            }

            [FunctionName(nameof(PlusOne))]
            public static int PlusOne([ActivityTrigger] int input) => input + 1;

            [FunctionName(nameof(FanOutFanIn))]
            public static async Task<string[]> FanOutFanIn(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                var tasks = new List<Task<string>>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(ctx.CallActivityAsync<string>(nameof(IntToString), i));
                }

                string[] results = await Task.WhenAll(tasks);
                Array.Sort(results);
                Array.Reverse(results);
                return results;
            }

            [FunctionName(nameof(IntToString))]
            public static string? IntToString([ActivityTrigger] int input) => input.ToString();

            [FunctionName(nameof(DeterministicGuid))]
            public static async Task<bool> DeterministicGuid([OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                Guid currentGuid1 = ctx.NewGuid();
                Guid originalGuid1 = await ctx.CallActivityAsync<Guid>(nameof(Echo), currentGuid1);
                if (currentGuid1 != originalGuid1)
                {
                    return false;
                }

                Guid currentGuid2 = ctx.NewGuid();
                Guid originalGuid2 = await ctx.CallActivityAsync<Guid>(nameof(Echo), currentGuid2);
                if (currentGuid2 != originalGuid2)
                {
                    return false;
                }

                return currentGuid1 != currentGuid2;
            }

            [FunctionName(nameof(Echo))]
            public static object Echo([ActivityTrigger] IDurableActivityContext ctx) => ctx.GetInput<object>();

            [FunctionName(nameof(OrchestrateCounterEntity))]
            public static async Task<int> OrchestrateCounterEntity(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                var entityId = new EntityId(nameof(Counter), ctx.NewGuid().ToString("N"));
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "add", 4);

                using (await ctx.LockAsync(entityId))
                {
                    int result = await ctx.CallEntityAsync<int>(entityId, "get");
                    return result;
                }
            }

            [FunctionName(nameof(Counter))]
            public static void Counter([EntityTrigger] IDurableEntityContext ctx)
            {
                int current = ctx.GetState<int>();
                switch (ctx.OperationName)
                {
                    case "incr":
                        ctx.SetState(current + 1);
                        break;
                    case "add":
                        int amount = ctx.GetInput<int>();
                        ctx.SetState(current + amount);
                        break;
                    case "get":
                        ctx.Return(current);
                        break;
                    case "set":
                        amount = ctx.GetInput<int>();
                        ctx.SetState(amount);
                        break;
                    case "delete":
                        ctx.DeleteState();
                        break;
                    default:
                        throw new NotImplementedException("No such entity operation");
                }
            }

            [FunctionName(nameof(WaitForEvent))]
            public static Task<object> WaitForEvent([OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                string name = ctx.GetInput<string>();
                return ctx.WaitForExternalEvent<object>(name);
            }

            [FunctionName(nameof(NoOp))]
            public static Task NoOp([OrchestrationTrigger] IDurableOrchestrationContext ctx) => Task.CompletedTask;
        }
    }
}
