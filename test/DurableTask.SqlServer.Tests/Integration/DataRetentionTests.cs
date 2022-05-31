// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.SqlServer.Logging;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    public class DataRetentionTests : IAsyncLifetime
    {
        readonly TestService testService;

        public DataRetentionTests(ITestOutputHelper output)
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
                inputGenerator: i => $"Hello, world {i}",
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

        /// <summary>
        /// Validates that external events sent to a completed orchestration are eventually removed from the database
        /// and that a log message is emitted for each discarded event.
        /// </summary>
        [Fact]
        public async Task EventsToCompletedOrchestrationAreDiscarded()
        {
            const int EventCount = 50;

            // Does nothing except return the original input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input: string.Empty,
                orchestrationName: "NoOp",
                implementation: (ctx, input) => Task.FromResult(input));
            await instance.WaitForCompletion();

            // Raise events to the completed instance
            await Enumerable.Range(0, EventCount).ParallelForEachAsync(maxConcurrency: 10, action: i =>
                instance.RaiseEventAsync("BogusEvent", i));

            // Check the logs to confirm that all the raised events were discarded
            static bool IsDeletedExternalEvent(LogEntry log)
            {
                if (log.EventId == EventIds.DiscardingEvent)
                {
                    LogAssert.FieldEquals(log, "EventType", EventType.EventRaised.ToString());
                    return true;
                }

                return false;
            }

            int discardedEventCount = 0;

            TimeSpan timeout = TimeSpan.FromSeconds(5).AdjustForDebugging();
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                discardedEventCount = this.testService.GetAndValidateLogs().Count(IsDeletedExternalEvent);

                // Make sure we're getting the exact count and not a larger count
                Assert.True(discardedEventCount <= EventCount);
                if (discardedEventCount == EventCount)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }

            Assert.Equal(EventCount, discardedEventCount);

            // Last, check the database to confirm there are no outstanding events.
            // The logs get written before the actual deletion happens, so wait an additional second to ensure that
            // the events had a chance to be deleted.
            await Task.Delay(TimeSpan.FromSeconds(1));
            int unprocessedEvents = await this.GetUnprocessedEventCountAsync();
            Assert.Equal(0, unprocessedEvents);
        }

        /// <summary>
        /// Verifies that canceled timers get properly cleaned up from the database.
        /// This test is a response to https://github.com/microsoft/durabletask-mssql/issues/93.
        /// </summary>
        [Fact]
        public async Task InvalidTimerEventsAreDiscarded()
        {
            var neverCompletingSource = new TaskCompletionSource();

            TestInstance<bool> instance = await this.testService.RunOrchestration(
                input: true,
                orchestrationName: "CreateAndInvalidateTimers",
                implementation: async (ctx, isFirstIteration) =>
                {
                    if (isFirstIteration)
                    {
                        using var cts = new CancellationTokenSource();

                        // This timer event will arrive for a different execution ID and is supposed to be discarded
                        _ = ctx.CreateTimer(ctx.CurrentUtcDateTime, string.Empty, cts.Token);
                        ctx.ContinueAsNew(false);
                        cts.Cancel();
                    }
                    else
                    {
                        // Prevent the orchestration from completing
                        await neverCompletingSource.Task;
                    }
                    
                    // Cancel the timer to allow the orchestration to continue
                    return Task.FromResult(isFirstIteration);
                });

            await instance.WaitForStart();

            TimeSpan timeout = TimeSpan.FromSeconds(5).AdjustForDebugging();
            Stopwatch sw = Stopwatch.StartNew();
            while (true)
            {
                int unprocessedEventCount = await this.GetUnprocessedEventCountAsync();
                if (unprocessedEventCount == 0)
                {
                    break;
                }

                Assert.True(sw.Elapsed < timeout, "Timeout expired waiting for unprocessed events to clear");
                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }
        }

        async Task<int> GetUnprocessedEventCountAsync()
        {
            string taskHubName = await this.testService.GetTaskHubNameAsync();
            int unprocessedEvents = (int)await SharedTestHelpers.ExecuteSqlAsync(
                $"SELECT COUNT(*) FROM dt.[NewEvents] WHERE TaskHub = '{taskHubName}'");
            return unprocessedEvents;
        }
    }
}
