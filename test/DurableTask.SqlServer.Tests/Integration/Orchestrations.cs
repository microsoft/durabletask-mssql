// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Moq;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    public class Orchestrations : IAsyncLifetime
    {
        readonly TestService testService;

        public Orchestrations(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync();

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task EmptyOrchestration()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "EmptyOrchestration";

            // Does nothing except return the original input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(
                expectedOutput: input);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
            LogAssert.Sequence(
                this.testService.LogProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName));
        }

        [Fact]
        public async Task SingleTimer()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "OrchestrationWithTimer";
            TimeSpan delay = TimeSpan.FromSeconds(3);

            // Performs a delay and then returns the input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: async (ctx, input) =>
                {
                    var result = await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input);
                    return result;
                });

            TimeSpan timeout = delay + TimeSpan.FromSeconds(10);
            OrchestrationState state = await instance.WaitForCompletion(
                timeout,
                expectedOutput: input);

            // Verify that the delay actually happened
            Assert.True(state.CreatedTime.Add(delay) <= state.CompletedTime);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
            LogAssert.Sequence(
                this.testService.LogProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName));
        }

        [Fact]
        public async Task IsReplaying()
        {
            TestInstance<string> instance = await this.testService.RunOrchestration<List<bool>, string>(
                null,
                orchestrationName: "TwoTimerReplayTester",
                implementation: async (ctx, _) =>
                {
                    var list = new List<bool>();
                    list.Add(ctx.IsReplaying);
                    await ctx.CreateTimer(DateTime.MinValue, 0);
                    list.Add(ctx.IsReplaying);
                    await ctx.CreateTimer(DateTime.MinValue, 0);
                    list.Add(ctx.IsReplaying);
                    return list;
                });

            OrchestrationState state = await instance.WaitForCompletion();
            JArray results = JArray.Parse(state.Output);
            Assert.Equal(3, results.Count);
            Assert.True((bool)results[0]);
            Assert.True((bool)results[1]);
            Assert.False((bool)results[2]);
        }

        [Fact]
        public async Task CurrentDateTimeUtc()
        {
            TestInstance<string> instance = await this.testService.RunOrchestration<bool, string>(
                null,
                orchestrationName: "CurrentDateTimeUtc",
                implementation: async (ctx, _) =>
                {
                    DateTime currentDate1 = ctx.CurrentUtcDateTime;
                    DateTime originalDate1 = await ctx.ScheduleTask<DateTime>("Echo", "", currentDate1);
                    if (currentDate1 != originalDate1)
                    {
                        return false;
                    }

                    DateTime currentDate2 = ctx.CurrentUtcDateTime;
                    DateTime originalDate2 = await ctx.ScheduleTask<DateTime>("Echo", "", currentDate2);
                    if (currentDate2 != originalDate2)
                    {
                        return false;
                    }

                    return currentDate1 != currentDate2;
                },
                activities: ("Echo", TestService.MakeActivity((TaskContext ctx, object input) => input)));

            OrchestrationState state = await instance.WaitForCompletion();
            Assert.True((bool)JToken.Parse(state.Output));
        }

        [Fact]
        public async Task SingleActivity()
        {
            string input = $"[{DateTime.UtcNow:o}]";

            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName: "OrchestrationWithActivity",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                activities: ("SayHello", TestService.MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")));

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: $"Hello, {input}!");
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        public async Task ActivityChain(int parallelCount)
        {
            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations<int, string>(
                parallelCount,
                instanceIdGenerator: _ => Guid.NewGuid().ToString("N"),
                inputGenerator: _ => null,
                orchestrationName: "OrchestrationsWithActivityChain",
                version: string.Empty,
                implementation: async (ctx, _) =>
                {
                    int value = 0;
                    for (int i = 0; i < 10; i++)
                    {
                        value = await ctx.ScheduleTask<int>("PlusOne", "", value);
                    }

                    return value;
                },
                activities: ("PlusOne", TestService.MakeActivity((TaskContext ctx, int input) => input + 1)));

            IEnumerable<Task> tasks = instances.Select(
                instance => instance.WaitForCompletion(
                    timeout: TimeSpan.FromSeconds(30),
                    expectedOutput: 10));
            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task LongRunningActivity()
        {
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(10);

            int activityExecutions = 0;
            TaskActivity longRunningActivity = TestService.MakeActivity((TaskContext ctx, string input) =>
            {
                Interlocked.Increment(ref activityExecutions);

                // Sleeping for 15 seconds should cause work-item renewal to trigger at least twice.
                Thread.Sleep(TimeSpan.FromSeconds(15));
                return input;
            });

            string input = $"[{DateTime.UtcNow:o}]";
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName: nameof(LongRunningActivity),
                implementation: (ctx, input) => ctx.ScheduleTask<string>("LongRunning", "", input),
                activities: ("LongRunning", longRunningActivity));

            OrchestrationState state = await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(30),
                expectedOutput: input);

            // Make sure the activity didn't get scheduled multiple times.
            Assert.Equal(1, activityExecutions);

            // Verify that the task renewal method was called at least once.
            this.testService.OrchestrationServiceMock.Verify(
                s => s.RenewTaskActivityWorkItemLockAsync(It.IsAny<TaskActivityWorkItem>()),
                Times.AtLeastOnce());
        }

        [Fact]
        public async Task OrchestrationException()
        {
            string errorMessage = "Kah-BOOOOOM!!!";

            // The exception is expected to fail the orchestration execution
            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName: "OrchestrationWithException",
                implementation: (ctx, input) => throw new Exception(errorMessage));

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(10),
                expectedOutput: errorMessage,
                expectedStatus: OrchestrationStatus.Failed);
        }

        [Fact]
        public async Task ActivityException()
        {
            // Performs a delay and then returns the input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Throw", ""),
                activities: new[] {
                    ("Throw", TestService.MakeActivity<string, string>((ctx, input) => throw new Exception("Kah-BOOOOOM!!!"))),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedStatus: OrchestrationStatus.Failed,
                expectedOutputRegex: ".*(Kah-BOOOOOM!!!).*");
        }

        [Fact]
        public async Task ActivityFanOut()
        {
            TestInstance<string> instance = await this.testService.RunOrchestration<string[], string>(
                null,
                orchestrationName: "OrchestrationWithActivityFanOut",
                implementation: async (ctx, _) =>
                {
                    var tasks = new List<Task<string>>();
                    for (int i = 0; i < 10; i++)
                    {
                        tasks.Add(ctx.ScheduleTask<string>("ToString", "", i));
                    }

                    string[] results = await Task.WhenAll(tasks);
                    Array.Sort(results);
                    Array.Reverse(results);
                    return results;
                },
                activities: new[] {
                    ("ToString", TestService.MakeActivity((TaskContext ctx, int input) => input.ToString())),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: new[] { "9", "8", "7", "6", "5", "4", "3", "2", "1", "0" });
        }

        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        public async Task ExternalEvents(int eventCount)
        {
            TaskCompletionSource<int> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<int, string>(
                null,
                orchestrationName: "OrchestrationWithExternalEvents",
                implementation: async (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<int>();

                    int i;
                    for (i = 0; i < eventCount; i++)
                    {
                        await tcs.Task;
                        tcs = new TaskCompletionSource<int>();
                    }

                    return i;
                },
                onEvent: (ctx, name, value) =>
                {
                    Assert.Equal("Event" + value, name);
                    tcs.TrySetResult(int.Parse(value));
                });

            for (int i = 0; i < eventCount; i++)
            {
                await instance.RaiseEventAsync($"Event{i}", i);
            }

            OrchestrationState state = await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: eventCount);
        }

        [Fact]
        public async Task Termination()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "OrchestrationWithTimer";
            TimeSpan delay = TimeSpan.FromSeconds(30);

            // Performs a delay and then returns the input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input));

            // Give the orchestration one second to start and then terminate it.
            // We wait to ensure that the log output we expect is deterministic.
            await instance.WaitForStart();
            await instance.TerminateAsync("Bye!");

            TimeSpan timeout = TimeSpan.FromSeconds(5);
            OrchestrationState state = await instance.WaitForCompletion(
                timeout,
                expectedStatus: OrchestrationStatus.Terminated,
                expectedOutput: "Bye!");

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
            LogAssert.Sequence(
                this.testService.LogProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName));
        }

        [Theory]
        [InlineData(10)]
        [InlineData(300)]
        public async Task ContinueAsNew(int lockTimeoutInSeconds)
        {
            // If the lock timeout is less than 60 seconds, then the dispatcher will also 
            // execute a code path that renews the task orchestration work item.
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds);

            TestInstance<int> instance = await this.testService.RunOrchestration(
                input: 0,
                orchestrationName: "ContinueAsNewTest",
                implementation: async (ctx, input) =>
                {
                    if (input < 10)
                    {
                        await ctx.CreateTimer<object>(DateTime.MinValue, null);
                        ctx.ContinueAsNew(input + 1);
                    }

                    return input;
                });

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: 10, continuedAsNew: true);
            Assert.NotNull(state.Input);
            Assert.Equal("10", state.Input);
        }

        [Fact]
        public async Task SubOrchestration()
        {
            string orchestrationName = "SubOrchestrationTest";
            TestInstance<int> testInstance = await this.testService.RunOrchestration(
                input: 1,
                orchestrationName,
                implementation: async (ctx, input) =>
                {
                    int result = 5;
                    if (input < 3)
                    {
                        int subResult =
                            await ctx.CreateSubOrchestrationInstance<int>(orchestrationName, string.Empty, input+1);
                        result += subResult;
                    }
                    return result;
                });
            await testInstance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15), expectedOutput: 15);
        }

        [Fact]
        public async Task VersionedOrchestration()
        {
            string orchestrationName = "VersionedOrchestrationTest";
            string version1 = "V1";
            string version2 = "V2";
            var waitTimeout = TimeSpan.FromSeconds(15);

            TestInstance<string> v1Instance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName,
                version: version1,
                implementation: (ctx, input) => Task.FromResult(version1));
            await v1Instance.WaitForCompletion(waitTimeout, expectedOutput: version1);

            TestInstance<string> v2Instance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName,
                version: version2,
                implementation: (ctx, input) => Task.FromResult(version2));
            await v2Instance.WaitForCompletion(waitTimeout, expectedOutput: version2);
        }

        [Fact]
        public async Task VersionedSubOrchestration()
        {
            string subOrchestrationName = "VersionedSubOrchestrationTest";
            string version1 = "V1";
            string version2 = "V2";
            var waitTimeout = TimeSpan.FromSeconds(30);

            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchestrationName, version1, implementation: (ctx, input) => Task.FromResult(version1));
            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchestrationName, version2, implementation: (ctx, input) => Task.FromResult(version2));
            
            TestInstance<string> parentInstance = await this.testService.RunOrchestration<string, string>(
                null,
                "ParentOrchestration",
                implementation: async (ctx, input) =>
                {
                    var result1 = await ctx.CreateSubOrchestrationInstance<string>(subOrchestrationName, version1, null);
                    var result2 = await ctx.CreateSubOrchestrationInstance<string>(subOrchestrationName, version2, null);
                    return result1 + result2;
                });
            await parentInstance.WaitForCompletion(waitTimeout, expectedOutput: version1 + version2);
        }

        [Fact]
        public async Task VersionedActivity()
        {
            string activityName = "VersionedActivityTest";
            string version1 = "V1";
            string version2 = "V2";
            var waitTimeout = TimeSpan.FromSeconds(30);

            this.testService.RegisterInlineActivity(
                activityName, version1, TestService.MakeActivity<string, string>((ctx, input) => version1));
            this.testService.RegisterInlineActivity(
                activityName, version2, TestService.MakeActivity<string, string>((ctx, input) => version2));
            
            var instance = await this.testService.RunOrchestration<string, string>(
                null,
                "OrchestrationWithVersionedActivities",
                implementation: async (ctx, input) =>
                {
                    var result1 = await ctx.ScheduleTask<string>(activityName, version1, input);
                    var result2 = await ctx.ScheduleTask<string>(activityName, version2, input);
                    return result1 + result2;
                });
            await instance.WaitForCompletion(waitTimeout, expectedOutput: version1 + version2);
        }
    }
}
