// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    public class PurgeTests : IAsyncLifetime
    {
        readonly TestService testService;

        public PurgeTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync();

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        /// <summary>
        /// This test validates that the purge behavior works correctly based on runtime status and time.
        /// </summary>
        [Theory]
        [InlineData(OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter)]
        [InlineData(OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter)]
        public async Task PurgesInstancesByStatus(OrchestrationStateTimeRangeFilterType filterType)
        {
            var events = new ConcurrentDictionary<string, TaskCompletionSource<bool>>();

            // Waits for an external event and then either completes or fails depending on that event
            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations(
                count: 30, // ideally some multiple of 3
                instanceIdGenerator: i => $"InstanceToPurge_{i:00}",
                inputGenerator: i=> $"Hello, world {i}",
                orchestrationName: "SimpleDelay",
                version: string.Empty,
                implementation: async (ctx, input) =>
                {
                    var tcs = new TaskCompletionSource<bool>();
                    events[ctx.OrchestrationInstance.InstanceId] = tcs;

                    bool shouldFail = await tcs.Task;
                    if (shouldFail)
                    {
                        throw new Exception("Kah-BOOOOOM!!!");
                    }

                    return shouldFail;
                },
                onEvent: (ctx, name, value) =>
                {
                    events[ctx.OrchestrationInstance.InstanceId].SetResult(bool.Parse(value));
                });

            await Task.WhenAll(instances.Select(instance => instance.WaitForStart()));

            // Try to purge the instance and check that it still exists
            await this.testService.PurgeAsync(DateTime.MaxValue, filterType);
            foreach (TestInstance<string> instance in instances)
            {
                OrchestrationState runningState = await instance.GetStateAsync();
                Assert.Equal(OrchestrationStatus.Running, runningState.OrchestrationStatus);
            }

            TimeSpan timeout = TimeSpan.FromSeconds(30);

            // We want to test a mix of completed, failed, and terminated instances to make sure they are all handled correctly.
            var tasks = new List<Task>();
            for (int i = 0; i < instances.Count; i++)
            {
                int index = i;
                tasks.Add(Task.Run(async () =>
                {
                    TestInstance<string> instance = instances[index];
                    if (index % 3 == 0)
                    {
                        // Complete the instance
                        await instance.RaiseEventAsync("Action", false);
                        await instance.WaitForCompletion(timeout, OrchestrationStatus.Completed);
                    }
                    else if (index % 3 == 1)
                    {
                        // Fail the instance
                        await instance.RaiseEventAsync("Action", true);
                        await instance.WaitForCompletion(timeout, OrchestrationStatus.Failed);
                    }
                    else
                    {
                        // Terminate the instance
                        await instance.TerminateAsync("Terminated!");
                        await instance.WaitForCompletion(timeout, OrchestrationStatus.Terminated);
                    }
                }));
            }

            // Wait for all instances to transition into their final state
            await Task.WhenAll(tasks);

            // This time-based purge should remove all the instances
            await this.testService.PurgeAsync(DateTime.MaxValue, filterType);
            foreach (TestInstance<string> instance in instances)
            {
                OrchestrationState purgedState = await instance.GetStateAsync();
                Assert.Null(purgedState);
            }

            // One more purge, just to make sure there are no failures when there is nothing left to purge
            await this.testService.PurgeAsync(DateTime.MaxValue, filterType);
        }
    }
}
