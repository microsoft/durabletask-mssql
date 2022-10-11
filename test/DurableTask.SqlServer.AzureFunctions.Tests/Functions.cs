// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

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

        [FunctionName(nameof(SubOrchestrationTest))]
        public static async Task<string> SubOrchestrationTest([OrchestrationTrigger] IDurableOrchestrationContext ctx)
        {
            await ctx.CallSubOrchestratorAsync(nameof(NoOp), "NoOpInstanceId", null);
            return "done";
        }

        [FunctionName(nameof(RewindOrchestration))]
        public static async Task<string> RewindOrchestration([OrchestrationTrigger] IDurableOrchestrationContext ctx)
        {
            return await ctx.CallActivityAsync<string>(nameof(CanFail), "activity");
        }

        [FunctionName(nameof(RewindSubOrchestration))]
        public static async Task<string> RewindSubOrchestration([OrchestrationTrigger] IDurableOrchestrationContext ctx)
        {
            var tasks = new List<Task<string>>();
            for (int i = 0; i < 3; i++)
            {
                tasks.Add(ctx.CallActivityAsync<string>(nameof(CanFail), i.ToString()));
            }
            tasks.Add(ctx.CallSubOrchestratorAsync<string>(nameof(RewindOrchestration), "RewindOrchestrationId", "suborchestration"));

            var results = await Task.WhenAll(tasks);
            return string.Join(',', results);
        }

        public static bool ThrowExceptionInCanFail { get; set; }

        [FunctionName(nameof(CanFail))]
        public static string CanFail([ActivityTrigger] IDurableActivityContext context)
        {
            if (ThrowExceptionInCanFail)
            {
                throw new Exception("exception");
            }

            return context.GetInput<string>();
        }
    }
}