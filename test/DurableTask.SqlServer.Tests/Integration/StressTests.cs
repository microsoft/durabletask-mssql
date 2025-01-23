// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Integration tests that are intended to reveal issues related to load or concurrency.
    /// These tests are expected to take longer to complete compared to functional integration
    /// tests and therefore may not be appropriate for all CI or rapid testing scenarios.
    /// </summary>
    [Trait("Category", "Stress")]
    [Collection("Integration")]
    public class StressTests : IAsyncLifetime
    {
        readonly TestService testService;

        public StressTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync();

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        // This test has previously been used to uncover various deadlock issues by stressing the code paths
        // related to foreign keys that point to the Instances and Payloads tables.
        // Example: https://github.com/microsoft/durabletask-mssql/issues/45 
        [Theory(Timeout = 1_000_000)]
        [InlineData(10)]
        [InlineData(2000)]
        public async Task ParallelSubOrchestrations(int subOrchestrationCount)
        {
            const string SubOrchestrationName = "SubOrchestration";
           
            this.testService.RegisterInlineOrchestration<DateTime, string>(
                orchestrationName: SubOrchestrationName,
                version: "",
                implementation: async (ctx, input) =>
                {
                    await ctx.CreateTimer(DateTime.MinValue, input);
                    return ctx.CurrentUtcDateTime;
                });

            TestInstance<int> testInstance = await this.testService.RunOrchestration(
                input: 1,
                orchestrationName: nameof(ParallelSubOrchestrations),
                implementation: async (ctx, input) =>
                {
                    var listInstances = new List<Task<DateTime>>();
                    for (int i = 0; i < subOrchestrationCount; i++)
                    {
                        Task<DateTime> instance = ctx.CreateSubOrchestrationInstance<DateTime>(
                            name: SubOrchestrationName,
                            version: "",
                            instanceId: $"suborchestration[{i}]",
                            input: $"{i}");
                        listInstances.Add(instance);
                    }

                    DateTime[] results = await Task.WhenAll(listInstances);
                    return new List<DateTime>(results);
                });

            // On a fast Windows desktop machine, a 2000 sub-orchestration test should complete in 30-40 seconds.
            // On slower CI machines, this test could take several minutes to complete.
            await testInstance.WaitForCompletion(TimeSpan.FromMinutes(5));
        }

        [Theory(Timeout = 100_000)]
        [InlineData(10)]
        public async Task ParallelWithBigPayload(int subOrchestrationCount)
        {
            const string SubOrchestrationName = "SubOrchestration";
            string bigString = string.Join("", Enumerable.Range(0, 1024 * 1024 * 10).Select(x => "1"));

            this.testService.RegisterInlineOrchestration<DateTime, string>(
                orchestrationName: SubOrchestrationName,
                version: "",
                implementation: async (ctx, input) =>
                {
                    await ctx.CreateTimer(DateTime.MinValue, input);
                    return ctx.CurrentUtcDateTime;
                });

            TestInstance<int> testInstance = await this.testService.RunOrchestration(
                input: 1,
                orchestrationName: nameof(ParallelSubOrchestrations),
                implementation: async (ctx, input) =>
                {
                    var listInstances = new List<Task<DateTime>>();
                    for (int i = 0; i < subOrchestrationCount; i++)
                    {
                        Task<DateTime> instance = ctx.CreateSubOrchestrationInstance<DateTime>(
                            name: SubOrchestrationName,
                            version: "",
                            instanceId: $"suborchestration[{i}]",
                            input: $"{i}-{bigString}");
                        listInstances.Add(instance);
                    }

                    DateTime[] results = await Task.WhenAll(listInstances);
                    return new List<DateTime>(results);
                });

            await testInstance.WaitForCompletion(TimeSpan.FromMinutes(1));
        }
    }
}
