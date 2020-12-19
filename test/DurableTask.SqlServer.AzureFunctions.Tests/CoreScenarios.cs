namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
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
        }
    }
}
