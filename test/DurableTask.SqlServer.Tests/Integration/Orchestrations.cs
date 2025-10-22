// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Moq;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using OpenTelemetry;
    using OpenTelemetry.Resources;
    using OpenTelemetry.Trace;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class Orchestrations : IAsyncLifetime
    {
        readonly TestService testService;
        readonly ITestOutputHelper outputHelper;

        public Orchestrations(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
            this.outputHelper = output;
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
        public async Task TimerCancellation()
        {
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input: "",
                orchestrationName: "TimerCancellationTest",
                implementation: async (ctx, input) =>
                {
                    using var cts = new CancellationTokenSource();
                    Task task = ctx.ScheduleTask<string>("SayHello", "", input);
                    Task timer = ctx.CreateTimer(ctx.CurrentUtcDateTime.AddSeconds(20), "", cts.Token);
                    Task winner = await Task.WhenAny(task, timer);
                    if (timer.IsCompleted)
                    {
                        return "Failed: timer shouldn't be completed!";
                    }

                    // Need to cancel the timer to allow the orchestration to complete
                    cts.Cancel();
                    return "done";
                },
                activities: ("SayHello", TestService.MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")));

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: $"done");
        }

        [Fact]
        public async Task UncaughtOrchestrationException()
        {
            var exceptionToThrow = new ApplicationException("Kah-BOOOOOM!!!");

            // The exception is expected to fail the orchestration execution
            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName: "OrchestrationWithException",
                implementation: (ctx, input) => throw exceptionToThrow);

            OrchestrationState state = await instance.WaitForCompletion(expectedStatus: OrchestrationStatus.Failed);
            Assert.NotNull(state.FailureDetails);
            Assert.Equal(exceptionToThrow.Message, state.FailureDetails.ErrorMessage);
            Assert.Equal(exceptionToThrow.GetType().FullName, state.FailureDetails.ErrorType);
            Assert.NotNull(state.FailureDetails.StackTrace);
        }

        [Fact]
        public async Task UncaughtActivityException()
        {
            var exceptionToThrow = new ApplicationException("Kah-BOOOOOM!!!");

            // Schedules a task that throws an uncaught exception
            TestInstance<string> instance = await this.testService.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Throw", ""),
                activities: new[] {
                    ("Throw", TestService.MakeActivity<string, string>((ctx, input) => throw exceptionToThrow)),
                });

            OrchestrationState state = await instance.WaitForCompletion(expectedStatus: OrchestrationStatus.Failed);
            Assert.NotNull(state.FailureDetails);
            Assert.Contains("Task 'Throw' (#0) failed with an unhandled exception", state.FailureDetails.ErrorMessage);
            Assert.Equal(typeof(TaskFailedException).FullName, state.FailureDetails.ErrorType);
            Assert.NotNull(state.FailureDetails.StackTrace);

            // Ideally, the original exception message would be included in the orchestrator output.
            // This would require a change in DurableTask.Core.
            //// Assert.Contains(exceptionToThrow.Message, state.FailureDetails.ErrorMessage);
        }

        [Fact]
        public async Task CatchActivityException()
        {
            var exceptionToThrow = new ApplicationException("Kah-BOOOOOM!!!");

            // Schedules a task that throws an exception, which is then caught by the orchestration
            TestInstance<string> instance = await this.testService.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: async (ctx, input) =>
                {
                    try
                    {
                        await ctx.ScheduleTask<string>("Throw", "");
                        return null; // not expected
                    }
                    catch (TaskFailedException e)
                    {
                        return e.FailureDetails;
                    }
                },
                activities: new[] {
                    ("Throw", TestService.MakeActivity<string, string>((ctx, input) => throw exceptionToThrow)),
                });

            OrchestrationState state = await instance.WaitForCompletion();

            // The output should be a FailureDetails object with information about the caught exception
            Assert.NotNull(state.Output);
            FailureDetails details = JsonConvert.DeserializeObject<FailureDetails>(state.Output);
            Assert.NotNull(details);
            Assert.Equal(exceptionToThrow.Message, details.ErrorMessage);
            Assert.Equal(exceptionToThrow.GetType().FullName, details.ErrorType);
            Assert.NotNull(details.StackTrace);
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
                            await ctx.CreateSubOrchestrationInstance<int>(orchestrationName, string.Empty, $"Sub{input}", input + 1);
                        result += subResult;
                    }
                    return result;
                });
            await testInstance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15), expectedOutput: 15);
        }

        [Fact]
        public async Task ListParentOrchestrationsOnly()
        {
            // Arrange: start an orchestration instance which starts a sub orchestration instance.
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
                            await ctx.CreateSubOrchestrationInstance<int>(orchestrationName, string.Empty, $"Sub{input}", input + 1);
                        result += subResult;
                    }
                    return result;
                });

            // Act: query orchestration instances to retrieve only parent instances.
            var filter = new SqlOrchestrationQuery { ExcludeSubOrchestrations = true };
            IReadOnlyCollection<OrchestrationState> list = await this.testService.OrchestrationServiceMock.Object.GetManyOrchestrationsAsync(filter, CancellationToken.None);

            // Assert: total number of started orchestrations is 2 but we expect to have only one main orchestration.
            Assert.Single(list);
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
                    string result1 = await ctx.CreateSubOrchestrationInstance<string>(subOrchestrationName, version1, null);
                    string result2 = await ctx.CreateSubOrchestrationInstance<string>(subOrchestrationName, version2, null);
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

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                null,
                "OrchestrationWithVersionedActivities",
                implementation: async (ctx, input) =>
                {
                    string result1 = await ctx.ScheduleTask<string>(activityName, version1, input);
                    string result2 = await ctx.ScheduleTask<string>(activityName, version2, input);
                    return result1 + result2;
                });
            await instance.WaitForCompletion(waitTimeout, expectedOutput: version1 + version2);
        }

        [Fact]
        public async Task RecreateCompletedInstance()
        {
            string input1 = $"Hello {Guid.NewGuid():N}";
            string input2 = $"Hello {Guid.NewGuid():N}";
            string orchestrationName = "EmptyOrchestration";
            string instanceId = "SINGLETON_" + Guid.NewGuid().ToString("N");

            // Does nothing except return the original input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input1,
                orchestrationName,
                version: null,
                instanceId,
                implementation: (ctx, input) => Task.FromResult(input));
            OrchestrationState state1 = await instance.WaitForCompletion(expectedOutput: input1);

            // Run the same instance again with the same instance ID
            await instance.RestartAsync(input2);
            OrchestrationState state2 = await instance.WaitForCompletion(expectedOutput: input2);

            // Confirm that the two states are sufficiently different
            Assert.NotEqual(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
            Assert.NotEqual(state1.Input, state2.Input);
            Assert.NotEqual(state1.CreatedTime, state2.CreatedTime);
            Assert.NotEqual(state1.CompletedTime, state2.CompletedTime);
        }

        [Fact]
        public async Task RecreateRunningInstance()
        {
            string input = $"Hello {Guid.NewGuid():N}";
            string orchestrationName = "EmptyOrchestration";
            string instanceId = "SINGLETON_" + Guid.NewGuid().ToString("N");
            TimeSpan delay = TimeSpan.FromSeconds(30);

            // Start an orchestration and wait for it to block
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                version: null,
                instanceId,
                implementation: (ctx, input) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input));
            await instance.WaitForStart();

            // Run the same instance again with the same instance ID
            InvalidOperationException exception = await Assert.ThrowsAsync<OrchestrationAlreadyExistsException>(
                () => instance.RestartAsync(input));
            Assert.Contains(instanceId, exception.Message);

            // This time allow overwriting pending and running instances.
            string oldExecutionId = instance.ExecutionId;
            await instance.RestartAsync(input, new[] { OrchestrationStatus.Canceled });
            Assert.NotEqual(oldExecutionId, instance.ExecutionId);
        }

        [Theory]
        //[InlineData(true)] // BUG: https://github.com/microsoft/durabletask-mssql/issues/148
        [InlineData(false)]
        public async Task RetryFailedSubOrchestration(bool userSpecifiedInstanceId)
        {
            string subOrchestrationName = "FlakeySubOrchestration";

            bool firstTime = true;
            this.testService.RegisterInlineOrchestration<bool, string>(
                subOrchestrationName, implementation: (ctx, input) =>
                {
                    if (firstTime)
                    {
                        firstTime = false;
                        throw new ApplicationException("Kah-BOOOOOM!!!");
                    }

                    return Task.FromResult(!firstTime);
                });

            string subOrchestratorInstanceIdOrNull = userSpecifiedInstanceId ? Guid.NewGuid().ToString("N") : null;

            TestInstance<string> parentInstance = await this.testService.RunOrchestration<bool, string>(
                null,
                "ParentOrchestration",
                implementation: async (ctx, input) =>
                {
                    return await ctx.CreateSubOrchestrationInstanceWithRetry<bool>(
                        name: subOrchestrationName,
                        version: null,
                        instanceId: subOrchestratorInstanceIdOrNull,
                        new RetryOptions(TimeSpan.FromMilliseconds(1), maxNumberOfAttempts: 2),
                        input: null);
                });
            await parentInstance.WaitForCompletion(expectedOutput: true);
        }

        [Fact]
        public async Task TraceContextFlowCorrectly()
        {
            string traceSourceName = "MyTraceSource";
            string orchestrationName = "ParentOrchestration";
            string subOrchestrationName = "MySubOrchestration";
            string activityName = "MyActivity";
            TimeSpan delay = TimeSpan.FromMilliseconds(500);

            // Use the OpenTelemetry SDK to collect traces from the Durable Task Framework. The in-memory
            // exporter allows us to capture the exported traces and validate them.
            var exportedItems = new List<Activity>();
            using TracerProvider tracerProvider = Sdk.CreateTracerProviderBuilder()
                .AddSource(traceSourceName, "DurableTask.Core")
                .ConfigureResource(r => r.AddService("Test"))
                .AddInMemoryExporter(exportedItems)
                .Build();

            using var traceSource = new ActivitySource(traceSourceName);
            using var clientSpan = traceSource.StartActivity("TestSpan");
            clientSpan.TraceStateString = "TestTraceState";

            // Flow:
            // [Orchestration] --> [SubOrchestration] --> [Activity]
            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchestrationName,
                implementation: (ctx, input) => ctx.ScheduleTask<string>(activityName, version: "", input));
            TestInstance<string> parentInstance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName,
                implementation: async (ctx, input) =>
                {
                    if (Activity.Current?.TraceStateString != null)
                    {
                        Activity.Current.TraceStateString += " (modified!)";
                    }

                    // Delay to test the duration of the final trace span
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), "");

                    return await ctx.CreateSubOrchestrationInstance<string>(subOrchestrationName, version: "", input);
                },
                activities: new[]
                {
                    (activityName, TestService.MakeActivity((TaskContext ctx, string input) =>
                    {
                        if (Activity.Current == null)
                        {
                            throw new ApplicationException("Activity.Current is null!");
                        }

                        Thread.Sleep(delay);
                        return string.Join('\n', Activity.Current.Id, Activity.Current.TraceStateString);
                    })),
                });

            OrchestrationState state = await parentInstance.WaitForCompletion();

            // The output is expected to be the trace activity (span) ID and the trace state. These are the fields we
            // mainly care about since those are the ones that get persisted in the database. This validation allows
            // us to prove that the activity code has access to the correct trace activity information.
            Assert.NotNull(state.Output);
            string[] parts = JsonConvert.DeserializeObject<string>(state.Output).Split('\n');
            Assert.Equal(2, parts.Length);

            string traceParent = parts[0];
            Assert.NotEqual(string.Empty, traceParent);
            string traceState = parts[1];
            Assert.Equal("TestTraceState (modified!)", traceState);

            // Verify that the trace IDs are the same and that the spans are different (client vs. activity span)
            ActivityContext outputSpan = ActivityContext.Parse(traceParent, traceState);
            Assert.Equal(clientSpan.TraceId, outputSpan.TraceId);
            Assert.NotEqual(clientSpan.SpanId, outputSpan.SpanId);

            clientSpan.Stop();
            tracerProvider.ForceFlush();

            foreach (Activity span in exportedItems)
            {
                this.outputHelper.WriteLine(
                    $"{span.Id}: Name={span.DisplayName}, Start={span.StartTimeUtc:o}, Duration={span.Duration}");
            }

            Assert.True(exportedItems.Count >= 4);

            // Validate the orchestration trace activity/span. Specifically, the IDs and time range.
            // We need to verify these because we use custom logic to store and retrieve this data (not serialization).
            Activity orchestratorSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"orchestration:{orchestrationName}");
            Assert.NotNull(orchestratorSpan);
            Assert.Equal(clientSpan.TraceId, orchestratorSpan.TraceId);
            Assert.NotEqual(clientSpan.SpanId, orchestratorSpan.SpanId); // new span ID
            Assert.Equal("TestTraceState", orchestratorSpan.TraceStateString);
            Assert.True(orchestratorSpan.StartTimeUtc >= clientSpan.StartTimeUtc);
            Assert.True(orchestratorSpan.Duration > delay * 2); // two sleeps
            Assert.True(orchestratorSpan.StartTimeUtc + orchestratorSpan.Duration <= clientSpan.StartTimeUtc + clientSpan.Duration);

            // Validate the sub-orchestrator span, which should be a sub-set of the parent orchestration span.
            Activity subOrchestratorSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"orchestration:{subOrchestrationName}");
            Assert.NotNull(subOrchestratorSpan);
            Assert.Equal(clientSpan.TraceId, subOrchestratorSpan.TraceId);
            Assert.NotEqual(orchestratorSpan.SpanId, subOrchestratorSpan.SpanId); // new span ID
            Assert.Equal("TestTraceState (modified!)", subOrchestratorSpan.TraceStateString);
            Assert.True(subOrchestratorSpan.StartTimeUtc > orchestratorSpan.StartTimeUtc + delay);
            Assert.True(subOrchestratorSpan.Duration > delay, $"Unexpected duration: {subOrchestratorSpan.Duration}");
            Assert.True(subOrchestratorSpan.Duration < delay * 2, $"Unexpected duration: {subOrchestratorSpan.Duration}");

            // Validate the activity span, which should be a subset of the sub-orchestration span
            Activity activitySpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"activity:{activityName}");
            Assert.NotNull(activitySpan);
            Assert.Equal(clientSpan.TraceId, activitySpan.TraceId);
            Assert.NotEqual(subOrchestratorSpan.SpanId, activitySpan.SpanId); // new span ID
            Assert.Equal("TestTraceState (modified!)", activitySpan.TraceStateString);
            Assert.True(activitySpan.StartTimeUtc > subOrchestratorSpan.StartTimeUtc);
            Assert.True(activitySpan.Duration < subOrchestratorSpan.Duration);
            Assert.True(activitySpan.Duration > delay);
            Assert.True(activitySpan.Duration < delay * 2);
        }

        [Fact]
        public async Task SuspendAndResumeInstance()
        {
            TaskCompletionSource<int> tcs = null;

            const int EventCount = 5;
            string orchestrationName = "SuspendResumeOrchestration";

            TestInstance<string> instance = await this.testService.RunOrchestration<int, string>(
                null,
                orchestrationName,
                implementation: async (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<int>();

                    int i;
                    for (i = 0; i < EventCount; i++)
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

            // Wait for the orchestration to finish starting
            await instance.WaitForStart();

            // Suspend the orchestration so that it won't process any new events
            await instance.SuspendAsync();

            // Raise the events, which should get buffered but not consumed
            for (int i = 0; i < EventCount; i++)
            {
                await instance.RaiseEventAsync($"Event{i}", i);
            }

            // Make sure that the orchestration *doesn't* complete
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => instance.WaitForCompletion(TimeSpan.FromSeconds(3), doNotAdjustTimeout: true));

            // Confirm that the orchestration is in a suspended state
            OrchestrationState state = await instance.GetStateAsync();
            Assert.Equal(OrchestrationStatus.Suspended, state.OrchestrationStatus);

            // Resume the orchestration
            await instance.ResumeAsync();

            // Now the orchestration should complete immediately
            await instance.WaitForCompletion(timeout: TimeSpan.FromSeconds(3), expectedOutput: EventCount);
        }

        [Fact]
        public async Task TerminateScheduledOrchestration()
        {
            string orchestrationName = "ScheduledOrchestration";

            // Does nothing except return the original input
            TestInstance<object> instance = await this.testService.RunOrchestration(
                input: (object)null,
                orchestrationName,
                version: null,
                instanceId: null,
                scheduledStartTime: DateTime.UtcNow.AddSeconds(30),
                implementation: (ctx, input) => Task.FromResult("done"));

            // Terminate the orchestration before it starts
            await instance.TerminateAsync("Bye!");

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(3),
                expectedStatus: OrchestrationStatus.Terminated,
                expectedOutput: "Bye!");

            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }
    }
}
