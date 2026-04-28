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
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;
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

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: input);

            // Verify backward compatibility: tags should be null when none are specified
            Assert.Null(state.Tags);

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

        // This regression locks down the expected DurableTask span shape for a simple
        // parent orchestration -> sub-orchestration -> activity flow. The important
        // nuance is that an activity under a sub-orchestration is nested via an
        // activity client span, so the hierarchy is:
        // parent orchestration server -> sub-orchestration client -> sub-orchestration server
        // -> activity client -> activity server.
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
                    $"{span.Id}: Name={span.DisplayName}, Kind={span.Kind}, Start={span.StartTimeUtc:o}, Duration={span.Duration}, TraceState={span.TraceStateString ?? "(null)"}");
            }

            Assert.True(exportedItems.Count >= 4);

            // Validate the orchestration trace activity/span. Specifically, the IDs and time range.
            // We need to verify these because we use custom logic to store and retrieve this data (not serialization).
            // Filter by Server kind to get the actual execution span, not the client-side scheduling span.
            Activity orchestratorSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"orchestration:{orchestrationName}" && span.Kind == ActivityKind.Server);
            Assert.NotNull(orchestratorSpan);
            Assert.Equal(clientSpan.TraceId, orchestratorSpan.TraceId);
            Assert.NotEqual(clientSpan.SpanId, orchestratorSpan.SpanId); // new span ID
            Assert.Equal("TestTraceState (modified!)", orchestratorSpan.TraceStateString);
            Assert.True(orchestratorSpan.StartTimeUtc >= clientSpan.StartTimeUtc);
            Assert.True(orchestratorSpan.Duration > delay * 2); // two sleeps
            Assert.True(orchestratorSpan.StartTimeUtc + orchestratorSpan.Duration <= clientSpan.StartTimeUtc + clientSpan.Duration);

            // Validate the sub-orchestrator span, which should be a sub-set of the parent orchestration span.
            Activity subOrchestratorSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"orchestration:{subOrchestrationName}" && span.Kind == ActivityKind.Server);
            Assert.NotNull(subOrchestratorSpan);
            Assert.Equal(clientSpan.TraceId, subOrchestratorSpan.TraceId);
            Assert.NotEqual(orchestratorSpan.SpanId, subOrchestratorSpan.SpanId); // new span ID
            Assert.Equal("TestTraceState (modified!)", subOrchestratorSpan.TraceStateString);
            Assert.True(subOrchestratorSpan.StartTimeUtc > orchestratorSpan.StartTimeUtc + delay);
            Assert.True(subOrchestratorSpan.Duration > delay, $"Unexpected duration: {subOrchestratorSpan.Duration}");
            Assert.True(subOrchestratorSpan.Duration < delay * 2, $"Unexpected duration: {subOrchestratorSpan.Duration}");

            Activity subOrchestratorClientSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"orchestration:{subOrchestrationName}" && span.Kind == ActivityKind.Client);
            Assert.NotNull(subOrchestratorClientSpan);

            // The sub-orchestration execution span hangs off the sub-orchestration client span,
            // which in turn hangs off the parent orchestration execution span.
            Assert.Equal(orchestratorSpan.SpanId, subOrchestratorClientSpan.ParentSpanId);
            Assert.Equal(subOrchestratorClientSpan.SpanId, subOrchestratorSpan.ParentSpanId);

            // Validate the activity span, which should be a subset of the sub-orchestration span
            Activity activitySpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"activity:{activityName}" && span.Kind == ActivityKind.Server);
            Assert.NotNull(activitySpan);
            Assert.Equal(clientSpan.TraceId, activitySpan.TraceId);
            Assert.NotEqual(subOrchestratorSpan.SpanId, activitySpan.SpanId); // new span ID
            Assert.Equal("TestTraceState (modified!)", activitySpan.TraceStateString);
            Assert.True(activitySpan.StartTimeUtc > subOrchestratorSpan.StartTimeUtc);
            Assert.True(activitySpan.Duration < subOrchestratorSpan.Duration);
            Assert.True(activitySpan.Duration > delay);
            Assert.True(activitySpan.Duration < delay * 2);

            Activity activityClientSpan = exportedItems.LastOrDefault(
                span => span.OperationName == $"activity:{activityName}" && span.Kind == ActivityKind.Client);
            Assert.NotNull(activityClientSpan);

            // This is the key shape for selling the trace fix: an activity scheduled by a
            // sub-orchestrator must remain inside that sub-orchestrator's execution span
            // hierarchy rather than floating at the root with a dangling parent.
            Assert.Equal(subOrchestratorSpan.SpanId, activityClientSpan.ParentSpanId);
            Assert.Equal(activityClientSpan.SpanId, activitySpan.ParentSpanId);
        }

        // This regression protects the SQL-specific bug where an orchestration reload could
        // generate a fresh execution span ID, leaving later activity completion and timer
        // spans pointing at a parent that no longer exists in the exported trace.
        [Fact]
        public async Task TraceContextMaintainsStableOrchestrationSpanAcrossContinuations()
        {
            string traceSourceName = "MyTraceSource";
            string orchestrationName = "ContinuationTraceContextOrchestration";
            string[] activityNames = { "FirstActivity", "SecondActivity" };
            TimeSpan delay = TimeSpan.FromMilliseconds(250);

            var exportedItems = new List<Activity>();
            using TracerProvider tracerProvider = Sdk.CreateTracerProviderBuilder()
                .AddSource(traceSourceName, "DurableTask.Core")
                .ConfigureResource(r => r.AddService("Test"))
                .AddInMemoryExporter(exportedItems)
                .Build();

            using var traceSource = new ActivitySource(traceSourceName);
            using var clientSpan = traceSource.StartActivity("TestSpan");
            clientSpan.TraceStateString = "TestTraceState";

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: "input",
                orchestrationName: orchestrationName,
                implementation: async (ctx, input) =>
                {
                    string first = await ctx.ScheduleTask<string>(activityNames[0], "", input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), "");
                    string second = await ctx.ScheduleTask<string>(activityNames[1], "", input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), "");
                    return string.Join(",", first, second);
                },
                activities: new[]
                {
                    (activityNames[0], TestService.MakeActivity((TaskContext ctx, string input) => $"first-{input}")),
                    (activityNames[1], TestService.MakeActivity((TaskContext ctx, string input) => $"second-{input}")),
                });

            await instance.WaitForCompletion(expectedOutput: "first-input,second-input");

            clientSpan.Stop();
            tracerProvider.ForceFlush();

            Activity[] orchestrationSpans = exportedItems
                .Where(span => span.OperationName == $"orchestration:{orchestrationName}" && span.Kind == ActivityKind.Server)
                .ToArray();
            Assert.NotEmpty(orchestrationSpans);

            ActivitySpanId[] orchestrationSpanIds = orchestrationSpans
                .Select(span => span.SpanId)
                .Distinct()
                .ToArray();
            Assert.Single(orchestrationSpanIds);

            ActivitySpanId orchestrationSpanId = orchestrationSpanIds[0];
            var activityOperationNames = new HashSet<string>(activityNames.Select(name => $"activity:{name}"));

            Activity[] completionSpans = exportedItems
                .Where(span => span.Kind == ActivityKind.Client && activityOperationNames.Contains(span.OperationName))
                .ToArray();
            Assert.Equal(2, completionSpans.Length);
            Assert.All(completionSpans, span => Assert.Equal(orchestrationSpanId, span.ParentSpanId));

            Activity[] timerSpans = exportedItems
                .Where(span => span.Kind == ActivityKind.Internal && span.OperationName == $"orchestration:{orchestrationName}:timer")
                .ToArray();
            Assert.Equal(2, timerSpans.Length);
            Assert.All(timerSpans, span => Assert.Equal(orchestrationSpanId, span.ParentSpanId));
        }

        // This test observes raw Activity objects directly, without relying on exporter
        // behavior, so it can fail fast if any emitted ParentSpanId refers to a span that
        // was never actually produced. The hierarchy intentionally exercises nested
        // sub-orchestrations plus an activity at the deepest level.
        [Fact]
        public async Task ActivityListenerCapturesNestedSubOrchestrationHierarchyWithoutMissingParents()
        {
            string traceSourceName = "ActivityListenerNestedTraceSource";
            string parentOrchestrationName = "ActivityListenerParentOrchestration";
            string childOrchestrationName = "ActivityListenerChildOrchestration";
            string grandchildOrchestrationName = "ActivityListenerGrandchildOrchestration";
            string[] activityNames =
            {
                "ActivityListenerParentActivity",
                "ActivityListenerChildActivity",
                "ActivityListenerGrandchildActivity",
                "ActivityListenerAfterSubOrchestrationActivity",
            };
            TimeSpan delay = TimeSpan.FromMilliseconds(150);

            using var listener = new ActivityCaptureListener(traceSourceName, "DurableTask.Core");
            using var traceSource = new ActivitySource(traceSourceName);
            using Activity incomingRequest = traceSource.StartActivity("IncomingRequest") ??
                throw new InvalidOperationException("Failed to start the incoming request activity.");

            this.testService.RegisterInlineOrchestration<string, string>(
                grandchildOrchestrationName,
                implementation: async (ctx, input) =>
                {
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    return await ctx.ScheduleTask<string>(activityNames[2], version: string.Empty, input);
                });

            this.testService.RegisterInlineOrchestration<string, string>(
                childOrchestrationName,
                implementation: async (ctx, input) =>
                {
                    string childActivity = await ctx.ScheduleTask<string>(activityNames[1], version: string.Empty, input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    string grandchild = await ctx.CreateSubOrchestrationInstance<string>(grandchildOrchestrationName, version: string.Empty, input);
                    return string.Join("|", childActivity, grandchild);
                });

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: "payload",
                orchestrationName: parentOrchestrationName,
                implementation: async (ctx, input) =>
                {
                    string parentActivity = await ctx.ScheduleTask<string>(activityNames[0], version: string.Empty, input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    string child = await ctx.CreateSubOrchestrationInstance<string>(childOrchestrationName, version: string.Empty, input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    string afterChild = await ctx.ScheduleTask<string>(activityNames[3], version: string.Empty, input);
                    return string.Join("|", parentActivity, child, afterChild);
                },
                activities: new[]
                {
                    (activityNames[0], TestService.MakeActivity((TaskContext ctx, string input) => $"parent-{input}")),
                    (activityNames[1], TestService.MakeActivity((TaskContext ctx, string input) => $"child-{input}")),
                    (activityNames[2], TestService.MakeActivity((TaskContext ctx, string input) => $"grandchild-{input}")),
                    (activityNames[3], TestService.MakeActivity((TaskContext ctx, string input) => $"after-{input}")),
                });

            await instance.WaitForCompletion(expectedOutput: "parent-payload|child-payload|grandchild-payload|after-payload");

            incomingRequest.Stop();

            CapturedActivity[] traceActivities = listener.GetActivities(incomingRequest.TraceId);
            this.WriteCapturedActivities(traceActivities);

            // The bug we are guarding against is "parentSpanId emitted, but the parent span
            // does not exist". This assertion checks exactly that against the raw listener feed.
            AssertNoMissingParents(traceActivities);

            CapturedActivity parentOrchestration = GetUniqueSpan(
                traceActivities,
                $"orchestration:{parentOrchestrationName}",
                ActivityKind.Server);
            CapturedActivity childOrchestrationClient = GetUniqueSpan(
                traceActivities,
                $"orchestration:{childOrchestrationName}",
                ActivityKind.Client);
            CapturedActivity childOrchestrationServer = GetUniqueSpan(
                traceActivities,
                $"orchestration:{childOrchestrationName}",
                ActivityKind.Server);
            CapturedActivity grandchildOrchestrationClient = GetUniqueSpan(
                traceActivities,
                $"orchestration:{grandchildOrchestrationName}",
                ActivityKind.Client);
            CapturedActivity grandchildOrchestrationServer = GetUniqueSpan(
                traceActivities,
                $"orchestration:{grandchildOrchestrationName}",
                ActivityKind.Server);
            CapturedActivity grandchildActivityClient = GetUniqueSpan(
                traceActivities,
                $"activity:{activityNames[2]}",
                ActivityKind.Client);
            CapturedActivity grandchildActivityServer = GetUniqueSpan(
                traceActivities,
                $"activity:{activityNames[2]}",
                ActivityKind.Server);

            // Expected nesting:
            // parent orchestration server
            //   -> child sub-orchestration client
            //     -> child sub-orchestration server
            //       -> grandchild sub-orchestration client
            //         -> grandchild sub-orchestration server
            //           -> grandchild activity client
            //             -> grandchild activity server
            Assert.Equal(parentOrchestration.SpanId, childOrchestrationClient.ParentSpanId);
            Assert.Equal(childOrchestrationClient.SpanId, childOrchestrationServer.ParentSpanId);
            Assert.Equal(childOrchestrationServer.SpanId, grandchildOrchestrationClient.ParentSpanId);
            Assert.Equal(grandchildOrchestrationClient.SpanId, grandchildOrchestrationServer.ParentSpanId);
            Assert.Equal(grandchildOrchestrationServer.SpanId, grandchildActivityClient.ParentSpanId);
            Assert.Equal(grandchildActivityClient.SpanId, grandchildActivityServer.ParentSpanId);
        }

        // This test covers sibling sub-orchestrations that run under the same parent. It
        // ensures each invocation gets its own client/server pair and that the activities
        // inside each child orchestration remain attached to the corresponding child span
        // instead of pointing to a missing or reused parent.
        [Fact]
        public async Task ActivityListenerCapturesRepeatedSubOrchestrationsWithoutMissingParents()
        {
            string traceSourceName = "ActivityListenerRepeatedTraceSource";
            string parentOrchestrationName = "ActivityListenerRepeatedParentOrchestration";
            string childOrchestrationName = "ActivityListenerRepeatedChildOrchestration";
            string childActivityName = "ActivityListenerRepeatedChildActivity";
            TimeSpan delay = TimeSpan.FromMilliseconds(125);

            using var listener = new ActivityCaptureListener(traceSourceName, "DurableTask.Core");
            using var traceSource = new ActivitySource(traceSourceName);
            using Activity incomingRequest = traceSource.StartActivity("IncomingRequest") ??
                throw new InvalidOperationException("Failed to start the incoming request activity.");

            this.testService.RegisterInlineOrchestration<string, string>(
                childOrchestrationName,
                implementation: async (ctx, input) =>
                {
                    string activityResult = await ctx.ScheduleTask<string>(childActivityName, version: string.Empty, input);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    return activityResult;
                });

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: "payload",
                orchestrationName: parentOrchestrationName,
                implementation: async (ctx, input) =>
                {
                    string first = await ctx.CreateSubOrchestrationInstance<string>(childOrchestrationName, version: string.Empty, input: $"{input}-1");
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), string.Empty);
                    string second = await ctx.CreateSubOrchestrationInstance<string>(childOrchestrationName, version: string.Empty, input: $"{input}-2");
                    return string.Join("|", first, second);
                },
                activities: new[]
                {
                    (childActivityName, TestService.MakeActivity((TaskContext ctx, string input) => $"child-{input}")),
                });

            await instance.WaitForCompletion(expectedOutput: "child-payload-1|child-payload-2");

            incomingRequest.Stop();

            CapturedActivity[] traceActivities = listener.GetActivities(incomingRequest.TraceId);
            this.WriteCapturedActivities(traceActivities);

            // Every non-root ParentSpanId emitted for this trace must resolve to another
            // captured span, otherwise Geneva can show the child span at the root.
            AssertNoMissingParents(traceActivities);

            CapturedActivity parentOrchestration = GetUniqueSpan(
                traceActivities,
                $"orchestration:{parentOrchestrationName}",
                ActivityKind.Server);
            CapturedActivity[] childOrchestrationClientSpans = GetDistinctSpans(
                traceActivities,
                $"orchestration:{childOrchestrationName}",
                ActivityKind.Client);
            CapturedActivity[] childOrchestrationServerSpans = GetDistinctSpans(
                traceActivities,
                $"orchestration:{childOrchestrationName}",
                ActivityKind.Server);
            CapturedActivity[] childActivityClientSpans = GetDistinctSpans(
                traceActivities,
                $"activity:{childActivityName}",
                ActivityKind.Client);
            CapturedActivity[] childActivityServerSpans = GetDistinctSpans(
                traceActivities,
                $"activity:{childActivityName}",
                ActivityKind.Server);

            Assert.Equal(2, childOrchestrationClientSpans.Length);
            Assert.Equal(2, childOrchestrationServerSpans.Length);
            Assert.Equal(2, childActivityClientSpans.Length);
            Assert.Equal(2, childActivityServerSpans.Length);

            // Each sibling sub-orchestration should attach to the same parent orchestration,
            // but keep its own distinct client/server/activity chain below that parent.
            Assert.All(childOrchestrationClientSpans, span => Assert.Equal(parentOrchestration.SpanId, span.ParentSpanId));

            var childOrchestrationClientIds = childOrchestrationClientSpans.Select(span => span.SpanId).ToHashSet();
            Assert.All(childOrchestrationServerSpans, span => Assert.Contains(span.ParentSpanId, childOrchestrationClientIds));

            var childOrchestrationServerIds = childOrchestrationServerSpans.Select(span => span.SpanId).ToHashSet();
            Assert.All(childActivityClientSpans, span => Assert.Contains(span.ParentSpanId, childOrchestrationServerIds));

            var childActivityClientIds = childActivityClientSpans.Select(span => span.SpanId).ToHashSet();
            Assert.All(childActivityServerSpans, span => Assert.Contains(span.ParentSpanId, childActivityClientIds));
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

            // Confirm that the orchestration is pending
            OrchestrationState state = await instance.GetStateAsync();
            Assert.Equal(OrchestrationStatus.Pending, state.OrchestrationStatus);

            // Terminate the orchestration before it starts
            await instance.TerminateAsync("Bye!");

            // Confirm the orchestration was terminated
            await instance.WaitForCompletion(
                expectedStatus: OrchestrationStatus.Terminated,
                expectedOutput: "Bye!");

            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task TerminateSuspendedOrchestration()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "OrchestrationWithTimer";
            TimeSpan delay = TimeSpan.FromSeconds(30);

            // Performs a delay and then returns the input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input));

            // Wait for the orchestration to finish starting
            await instance.WaitForStart();

            // Suspend the orchestration so that it won't process any new events
            await instance.SuspendAsync();

            // Wait for the orchestration to become suspended
            OrchestrationState state = await instance.GetStateAsync();
            TimeSpan waitForSuspendTimeout = TimeSpan.FromSeconds(5);
            using CancellationTokenSource cts = new(waitForSuspendTimeout);
            while (!cts.IsCancellationRequested && state.OrchestrationStatus != OrchestrationStatus.Suspended)
            {
                state = await instance.GetStateAsync();
            }
            Assert.Equal(OrchestrationStatus.Suspended, state.OrchestrationStatus);

            // Now terminate the orchestration
            await instance.TerminateAsync("Bye!");

            TimeSpan waitForTerminationTimeout = TimeSpan.FromSeconds(5);
            state = await instance.WaitForCompletion(
                waitForTerminationTimeout,
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
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName));
        }

        [Fact]
        public async Task OrchestrationWithTags()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
            };

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "OrchestrationWithTags",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            Assert.NotNull(state.Tags);
            Assert.Equal(2, state.Tags.Count);
            Assert.Equal("value1", state.Tags["key1"]);
            Assert.Equal("value2", state.Tags["key2"]);

            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task OrchestrationWithEmptyTags()
        {
            string input = $"Hello {DateTime.UtcNow:o}";

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "OrchestrationWithEmptyTags",
                tags: new Dictionary<string, string>(),
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            Assert.Null(state.Tags);

            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task TagsSurviveContinueAsNew()
        {
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
            };

            TestInstance<int> instance = await this.testService.RunOrchestrationWithTags(
                input: 0,
                orchestrationName: "TagsContinueAsNewTest",
                tags: tags,
                implementation: async (ctx, input) =>
                {
                    if (input < 3)
                    {
                        await ctx.CreateTimer<object>(DateTime.MinValue, null);
                        ctx.ContinueAsNew(input + 1);
                    }

                    return input;
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: 3,
                continuedAsNew: true);

            Assert.NotNull(state.Tags);
            Assert.Equal(2, state.Tags.Count);
            Assert.Equal("value1", state.Tags["key1"]);
            Assert.Equal("value2", state.Tags["key2"]);
        }

        [Fact]
        public async Task SubOrchestrationInheritsTags()
        {
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
            };

            string subOrchestrationName = "SubOrchestrationForTagTest";
            string subInstanceId = $"sub-{Guid.NewGuid():N}";

            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchestrationName,
                implementation: (ctx, input) => Task.FromResult("done"));

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: (string)null,
                orchestrationName: "ParentOrchestrationForTagTest",
                tags: tags,
                implementation: async (ctx, input) =>
                {
                    return await ctx.CreateSubOrchestrationInstance<string>(
                        subOrchestrationName, string.Empty, subInstanceId, null);
                });

            OrchestrationState state = await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: "done");

            // Verify parent orchestration tags
            Assert.NotNull(state.Tags);
            Assert.Equal("value1", state.Tags["key1"]);
            Assert.Equal("value2", state.Tags["key2"]);

            // Verify sub-orchestration inherited the tags
            OrchestrationState subState = await this.testService.GetOrchestrationStateAsync(subInstanceId);
            Assert.NotNull(subState);
            Assert.NotNull(subState.Tags);
            Assert.Equal("value1", subState.Tags["key1"]);
            Assert.Equal("value2", subState.Tags["key2"]);
        }

        [Fact]
        public async Task TagsWithSpecialCharacters()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            var tags = new Dictionary<string, string>
            {
                { "key with spaces", "value with spaces" },
                { "unicode-key-日本語", "unicode-value-中文" },
                { "special\"chars", "value'with\"quotes" },
            };

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "TagsSpecialCharsTest",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            Assert.NotNull(state.Tags);
            Assert.Equal(3, state.Tags.Count);
            Assert.Equal("value with spaces", state.Tags["key with spaces"]);
            Assert.Equal("unicode-value-中文", state.Tags["unicode-key-日本語"]);
            Assert.Equal("value'with\"quotes", state.Tags["special\"chars"]);
        }

        [Fact]
        public async Task SubOrchestrationMergesTags()
        {
            // Parent tags
            var parentTags = new Dictionary<string, string>
            {
                { "env", "prod" },
                { "shared", "parent-value" },
            };

            // Sub-orchestration-specific tags (will be merged with parent tags by Core)
            var subTags = new Dictionary<string, string>
            {
                { "team", "backend" },
                { "shared", "child-override" },  // should override parent's value
            };

            string subOrchName = "SubOrchForMergeTest";
            string subInstanceId = $"sub-merge-{Guid.NewGuid():N}";

            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchName,
                implementation: (ctx, input) => Task.FromResult("done"));

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: (string)null,
                orchestrationName: "ParentOrchForMergeTest",
                tags: parentTags,
                implementation: async (ctx, input) =>
                {
                    // Use the 5-arg overload that passes sub-orch-specific tags
                    return await ctx.CreateSubOrchestrationInstance<string>(
                        subOrchName, string.Empty, subInstanceId, null, subTags);
                });

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: "done");

            // Verify sub-orchestration has MERGED tags (parent + child, child overrides)
            OrchestrationState subState = await this.testService.GetOrchestrationStateAsync(subInstanceId);
            Assert.NotNull(subState);
            Assert.NotNull(subState.Tags);
            Assert.Equal("prod", subState.Tags["env"]);            // inherited from parent
            Assert.Equal("backend", subState.Tags["team"]);        // from sub-orch
            Assert.Equal("child-override", subState.Tags["shared"]); // child overrides parent
        }

        [Fact]
        public async Task MultipleSubOrchestrationsMergeDifferentTags()
        {
            var parentTags = new Dictionary<string, string>
            {
                { "env", "prod" },
            };

            string subOrchName = "SubOrchForFanOutTest";
            string subId1 = $"sub-fanout1-{Guid.NewGuid():N}";
            string subId2 = $"sub-fanout2-{Guid.NewGuid():N}";

            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchName,
                implementation: (ctx, input) => Task.FromResult("done"));

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: (string)null,
                orchestrationName: "ParentOrchForFanOutTest",
                tags: parentTags,
                implementation: async (ctx, input) =>
                {
                    // Fan-out: create two sub-orchestrations with different tags
                    var tags1 = new Dictionary<string, string> { { "region", "us" } };
                    var tags2 = new Dictionary<string, string> { { "region", "eu" } };

                    Task<string> t1 = ctx.CreateSubOrchestrationInstance<string>(
                        subOrchName, string.Empty, subId1, null, tags1);
                    Task<string> t2 = ctx.CreateSubOrchestrationInstance<string>(
                        subOrchName, string.Empty, subId2, null, tags2);

                    await Task.WhenAll(t1, t2);
                    return "done";
                });

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: "done");

            // Verify each sub-orchestration got its own correctly-merged tags
            OrchestrationState sub1 = await this.testService.GetOrchestrationStateAsync(subId1);
            Assert.NotNull(sub1?.Tags);
            Assert.Equal("prod", sub1.Tags["env"]);   // inherited from parent
            Assert.Equal("us", sub1.Tags["region"]);   // specific to sub-orch 1

            OrchestrationState sub2 = await this.testService.GetOrchestrationStateAsync(subId2);
            Assert.NotNull(sub2?.Tags);
            Assert.Equal("prod", sub2.Tags["env"]);   // inherited from parent
            Assert.Equal("eu", sub2.Tags["region"]);   // specific to sub-orch 2
        }

        [Fact]
        public async Task MergedTagsExceedMaxSize_OversizedTagsDropped()
        {
            // Parent and child tags are each within the 8000-char limit,
            // but exceed it after Core's MergeTags() combines them.
            // Expected behavior: oversized merged tags are silently dropped
            // (with a trace warning), the sub-orchestration is created with
            // null tags, and the parent completes normally.

            var parentTags = new Dictionary<string, string>
            {
                { "parentKey", new string('p', 4500) },
            };

            var childTags = new Dictionary<string, string>
            {
                { "childKey", new string('c', 4500) },
            };

            string subOrchName = "SubOrchForOverflowTest";
            string subInstanceId = $"sub-overflow-{Guid.NewGuid():N}";

            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchName,
                implementation: (ctx, input) => Task.FromResult("done"));

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: (string)null,
                orchestrationName: "ParentOrchForOverflowTest",
                tags: parentTags,
                implementation: async (ctx, input) =>
                {
                    return await ctx.CreateSubOrchestrationInstance<string>(
                        subOrchName, string.Empty, subInstanceId, null, childTags);
                });

            // Parent should complete normally (sub-orch returns "done")
            await instance.WaitForCompletion(expectedOutput: "done");

            // Sub-orchestration should have been created, but with null tags
            // because the merged tags exceeded the maximum size.
            OrchestrationState subState = await this.testService.GetOrchestrationStateAsync(subInstanceId);
            Assert.NotNull(subState);
            Assert.Null(subState.Tags);
        }

        [Fact]
        public async Task TagsOnManyOrchestrations()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
            };

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "TagsManyQueryTest",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(expectedOutput: input);

            var filter = new SqlOrchestrationQuery();
            IReadOnlyCollection<OrchestrationState> results =
                await this.testService.OrchestrationServiceMock.Object.GetManyOrchestrationsAsync(
                    filter, CancellationToken.None);

            Assert.NotEmpty(results);
            bool foundTaggedInstance = false;
            foreach (OrchestrationState result in results)
            {
                if (result.OrchestrationInstance.InstanceId == instance.InstanceId)
                {
                    Assert.NotNull(result.Tags);
                    Assert.Equal("value1", result.Tags["key1"]);
                    foundTaggedInstance = true;
                }
            }

            Assert.True(foundTaggedInstance, "Did not find the tagged orchestration instance in query results.");
        }

        /// <summary>
        /// Verifies that orchestration tags propagate to the activity middleware context
        /// via OrchestrationExecutionContext, surviving the SQL persistence round-trip
        /// through the NewTasks table. This test exposes the gap where tags were
        /// serialized by TaskOrchestrationDispatcher but never persisted/restored
        /// by the MSSQL backend.
        /// </summary>
        [Fact]
        public async Task ActivityReceivesOrchestrationTags()
        {
            var tags = new Dictionary<string, string>
            {
                { "env", "test" },
                { "team", "platform" },
            };

            // Capture tags seen by activity middleware
            IDictionary<string, string> capturedTags = null;

            this.testService.AddActivityDispatcherMiddleware(async (context, next) =>
            {
                var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
                capturedTags = scheduledEvent?.Tags;
                await next();
            });

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "hello",
                orchestrationName: "ActivityTagsPropagation",
                tags: tags,
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Echo", "", input),
                activities: new[]
                {
                    ("Echo", TestService.MakeActivity((TaskContext ctx, string input) => input)),
                });

            await instance.WaitForCompletion(expectedOutput: "hello");

            // Verify the activity middleware received the orchestration's tags
            Assert.NotNull(capturedTags);
            Assert.Equal(2, capturedTags.Count);
            Assert.Equal("test", capturedTags["env"]);
            Assert.Equal("platform", capturedTags["team"]);
        }

        /// <summary>
        /// Verifies that per-activity tags (via ScheduleTaskOptions.Tags) are merged flat
        /// with orchestration-level tags, and that activity tags override on key collision.
        /// Both OrchestrationExecutionContext and TaskScheduledEvent carry the merged result.
        /// </summary>
        [Fact]
        public async Task ActivityTagsMergedWithOrchestrationTags()
        {
            var orchestrationTags = new Dictionary<string, string>
            {
                { "env", "prod" },
                { "team", "platform" },
            };

            // Capture tags seen by activity middleware via TaskScheduledEvent.Tags
            IDictionary<string, string> capturedTags = null;

            this.testService.AddActivityDispatcherMiddleware(async (context, next) =>
            {
                var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
                capturedTags = scheduledEvent?.Tags;
                await next();
            });

            var activityOptions = ScheduleTaskOptions.CreateBuilder()
                .AddTag("priority", "high")
                .AddTag("env", "staging") // overrides orchestration-level "env"
                .Build();

            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "hello",
                orchestrationName: "MergedActivityTags",
                tags: orchestrationTags,
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Echo", "", activityOptions, input),
                activities: new[]
                {
                    ("Echo", TestService.MakeActivity((TaskContext ctx, string input) => input)),
                });

            await instance.WaitForCompletion(expectedOutput: "hello");

            // Verify merged tags: activity "env=staging" overrides orchestration "env=prod"
            Assert.NotNull(capturedTags);
            Assert.Equal(3, capturedTags.Count);
            Assert.Equal("staging", capturedTags["env"]);
            Assert.Equal("platform", capturedTags["team"]);
            Assert.Equal("high", capturedTags["priority"]);
        }

        void WriteCapturedActivities(IEnumerable<CapturedActivity> activities)
        {
            foreach (CapturedActivity activity in activities.OrderBy(a => a.StartTimeUtc))
            {
                this.outputHelper.WriteLine(
                    $"{activity.TraceId}/{activity.SpanId} parent={activity.ParentSpanId} kind={activity.Kind} source={activity.SourceName} op={activity.OperationName}");
            }
        }

        static void AssertNoMissingParents(IReadOnlyCollection<CapturedActivity> activities)
        {
            var knownSpanIds = activities.Select(activity => activity.SpanId).ToHashSet();
            CapturedActivity[] missingParents = activities
                .Where(activity => activity.ParentSpanId != default && !knownSpanIds.Contains(activity.ParentSpanId))
                .ToArray();

            Assert.True(
                missingParents.Length == 0,
                "Captured activities have missing parents:" + Environment.NewLine +
                string.Join(
                    Environment.NewLine,
                    missingParents.Select(activity =>
                        $"{activity.OperationName} [{activity.Kind}] span={activity.SpanId} parent={activity.ParentSpanId}")));
        }

        static CapturedActivity GetUniqueSpan(
            IEnumerable<CapturedActivity> activities,
            string operationName,
            ActivityKind kind)
        {
            CapturedActivity[] matchingSpans = GetDistinctSpans(activities, operationName, kind);
            Assert.Single(matchingSpans);
            return matchingSpans[0];
        }

        static CapturedActivity[] GetDistinctSpans(
            IEnumerable<CapturedActivity> activities,
            string operationName,
            ActivityKind kind)
        {
            return activities
                .Where(activity => activity.OperationName == operationName && activity.Kind == kind)
                .GroupBy(activity => activity.SpanId)
                .Select(group => group.OrderByDescending(activity => activity.Duration).First())
                .ToArray();
        }

        sealed class ActivityCaptureListener : IDisposable
        {
            readonly object sync = new object();
            readonly HashSet<string> sourceNames;
            readonly List<CapturedActivity> activities = new List<CapturedActivity>();
            readonly ActivityListener listener;

            public ActivityCaptureListener(params string[] sourceNames)
            {
                this.sourceNames = new HashSet<string>(sourceNames, StringComparer.Ordinal);
                this.listener = new ActivityListener
                {
                    ShouldListenTo = source => this.sourceNames.Contains(source.Name),
                    Sample = static (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                    SampleUsingParentId = static (ref ActivityCreationOptions<string> options) => ActivitySamplingResult.AllDataAndRecorded,
                    ActivityStopped = activity =>
                    {
                        lock (this.sync)
                        {
                            this.activities.Add(CapturedActivity.From(activity));
                        }
                    },
                };

                ActivitySource.AddActivityListener(this.listener);
            }

            public CapturedActivity[] GetActivities(ActivityTraceId traceId)
            {
                lock (this.sync)
                {
                    return this.activities.Where(activity => activity.TraceId == traceId).ToArray();
                }
            }

            public void Dispose() => this.listener.Dispose();
        }

        sealed record CapturedActivity(
            string SourceName,
            string OperationName,
            ActivityKind Kind,
            ActivityTraceId TraceId,
            ActivitySpanId SpanId,
            ActivitySpanId ParentSpanId,
            DateTime StartTimeUtc,
            TimeSpan Duration)
        {
            public static CapturedActivity From(Activity activity)
            {
                return new CapturedActivity(
                    activity.Source.Name,
                    activity.OperationName,
                    activity.Kind,
                    activity.TraceId,
                    activity.SpanId,
                    activity.ParentSpanId,
                    activity.StartTimeUtc,
                    activity.Duration);
            }
        }
    }
}
