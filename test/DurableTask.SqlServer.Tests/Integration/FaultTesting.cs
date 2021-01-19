// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;

    public class FaultTesting : IAsyncLifetime
    {
        readonly TestService testService;

        public FaultTesting(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync();

        Task IAsyncLifetime.DisposeAsync() => this.testService?.DisposeAsync();

        /// <summary>
        /// Verifies that the orchestration correctly retries after a single faulted checkpoint.
        /// </summary>
        [Fact]
        public async Task CheckpointOrchestrationFault()
        {
            // This test needs to wait for the lock timeout to expire after the injected fault
            // before it will retry the checkpoint.
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(5);

            // Set up a mock that will fail the first call, but succeed the second call
            bool faultedOnce = false;
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.CompleteTaskOrchestrationWorkItemAsync(
                    It.IsAny<TaskOrchestrationWorkItem>(),
                    It.IsAny<OrchestrationRuntimeState>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<TaskMessage>(),
                    It.IsAny<OrchestrationState>()))
                .Callback(() =>
                {
                    if (!faultedOnce)
                    {
                        faultedOnce = true;
                        throw new Exception("Kah-BOOOOM!!!");
                    }
                });

            // Does nothing except return the original input
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input: "Hello, world!",
                orchestrationName: "EmptyOrchestration",
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion();
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.True(faultedOnce);

            // Verify that the CompleteTaskOrchestrationWorkItemAsync method was called exactly twice
            this.testService.OrchestrationServiceMock.Verify(svc => svc.CompleteTaskOrchestrationWorkItemAsync(
                It.IsAny<TaskOrchestrationWorkItem>(),
                It.IsAny<OrchestrationRuntimeState>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<TaskMessage>(),
                It.IsAny<OrchestrationState>()), Times.Exactly(2));
        }

        /// <summary>
        /// Verifies that when an orchestration hangs, the triggering event gets retried successfully,
        /// and that the "unstuck" execution fails to checkpoint due to a detected duplicate execution.
        /// </summary>
        [Fact]
        public async Task OrchestrationHang()
        {
            // This test needs to wait for the lock timeout to expire after the injected fault
            // before it will retry the checkpoint.
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(5);

            var resumeSignal = new ManualResetEvent(initialState: false);

            // Set up a mock that will hang on the first call, but succeed the second call
            bool firstCall = true;
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.CompleteTaskOrchestrationWorkItemAsync(
                    It.IsAny<TaskOrchestrationWorkItem>(),
                    It.IsAny<OrchestrationRuntimeState>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<IList<TaskMessage>>(),
                    It.IsAny<TaskMessage>(),
                    It.IsAny<OrchestrationState>()))
                .Callback(() =>
                {
                    if (firstCall)
                    {
                        firstCall = false;
                        resumeSignal.WaitOne();
                    }
                });

            // Block background work-item renewal to ensure another thread tries to execute the orchestration work item
            this.testService.OrchestrationServiceMock
                .Setup(svc => svc.ReleaseTaskOrchestrationWorkItemAsync(It.IsAny<TaskOrchestrationWorkItem>()))
                .Callback(() => resumeSignal.WaitOne());

            // Does nothing except return the original input
            string orchestrationName = "EmptyOrchestration";
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input: "Hello, world!",
                orchestrationName,
                implementation: (ctx, input) => Task.FromResult(input));

            // Give the orchestration extra time to finish. Time is needed for the lock timeout
            // to expire.
            await instance.WaitForCompletion(TimeSpan.FromSeconds(30));

            // Unblock the hung thread. This should result in a failure due to a detected duplicate execution.
            resumeSignal.Set();

            // Give the unblocked thread time to execute its logic and fail.
            await Task.Delay(TimeSpan.FromSeconds(3));

            // Verify that the CompleteTaskOrchestrationWorkItemAsync method was called exactly twice, and that the second call failed
            this.testService.OrchestrationServiceMock.Verify(svc => svc.CompleteTaskOrchestrationWorkItemAsync(
                It.IsAny<TaskOrchestrationWorkItem>(),
                It.IsAny<OrchestrationRuntimeState>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<IList<TaskMessage>>(),
                It.IsAny<TaskMessage>(),
                It.IsAny<OrchestrationState>()), Times.Exactly(2));

            // Check logs to confirm that a duplicate execution was detected
            LogAssert.Contains(
                this.testService.LogProvider,
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.DuplicateExecutionDetected(orchestrationName));
        }

        /// <summary>
        /// Verifies that when an activity task fails to checkpoint, the triggering event gets retried successfully.
        /// </summary>
        [Fact]
        public async Task ActivityCheckpointFault()
        {
            // This test needs to wait for the lock timeout to expire after the injected fault
            // before it will retry the checkpoint.
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(5);

            // Set up a mock that will hang on the first call, but succeed the second call
            bool firstCall = true;
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.CompleteTaskActivityWorkItemAsync(
                    It.IsAny<TaskActivityWorkItem>(),
                    It.IsAny<TaskMessage>()))
                .Callback(() =>
                {
                    if (firstCall)
                    {
                        firstCall = false;
                        throw new Exception("Kah-BOOOOM!!!");
                    }
                });

            // Run an orchestration with a single activity function call
            string orchestrationName = "OrchestrationWithActivity";
            string input = $"[{DateTime.UtcNow:o}]";
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                activities: ("SayHello", TestService.MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")));

            // Give the orchestration extra time to finish. Time is needed for the lock timeout
            // to expire.
            OrchestrationState state = await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(30),
                expectedOutput: $"Hello, {input}!");

            Assert.False(firstCall);

            // Verify that the CompleteTaskActivityWorkItemAsync method was called exactly twice, and that the second call failed
            this.testService.OrchestrationServiceMock.Verify(
                svc => svc.CompleteTaskActivityWorkItemAsync(
                    It.IsAny<TaskActivityWorkItem>(),
                    It.IsAny<TaskMessage>()),
                Times.Exactly(2));
        }


        /// <summary>
        /// Simulates a hang in task activity processing and verifies that the activity can be successfully resumed.
        /// </summary>
        [Fact]
        public async Task ActivityHang()
        {
            // This test needs to wait for the lock timeout to expire after the injected fault
            // before it will retry the checkpoint.
            this.testService.OrchestrationServiceOptions.WorkItemLockTimeout = TimeSpan.FromSeconds(5);

            var secondCallSignal = new ManualResetEvent(initialState: false);
            var resumeSignal = new ManualResetEvent(initialState: false);

            // Set up a mock that will hang on the first call, but succeed the second call
            bool firstCall = true;
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.CompleteTaskActivityWorkItemAsync(
                    It.IsAny<TaskActivityWorkItem>(),
                    It.IsAny<TaskMessage>()))
                .Callback(() =>
                {
                    if (firstCall)
                    {
                        firstCall = false;
                        resumeSignal.WaitOne();
                    }
                    else
                    {
                        secondCallSignal.Set();
                    }
                });

            // Block background activity renewal to ensure another thread tries to execute the activity work item
            this.testService.OrchestrationServiceMock
                .Setup(svc => svc.RenewTaskActivityWorkItemLockAsync(It.IsAny<TaskActivityWorkItem>()))
                .Callback(() => resumeSignal.WaitOne());

            // Run an orchestration with a single activity function call
            string orchestrationName = "OrchestrationWithActivity";
            string input = $"[{DateTime.UtcNow:o}]";
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName,
                implementation: async (ctx, input) =>
                {
                    string result = await ctx.ScheduleTask<string>("SayHello", "", input);
                    await Task.Delay(Timeout.Infinite); // simulate waiting for an external event
                    return result;
                },
                activities: ("SayHello", TestService.MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")));

            // Give the orchestration extra time to finish. Time is needed for the lock timeout
            // to expire.
            await instance.WaitForStart(TimeSpan.FromSeconds(10));
            bool resumed = secondCallSignal.WaitOne(TimeSpan.FromSeconds(10));

            Assert.True(resumed);
            Assert.False(firstCall);

            // Give the retrying thread time to checkpoint its progress
            await Task.Delay(TimeSpan.FromSeconds(3));

            // Unblock the hung thread. This should result in a failure due to a detected duplicate execution.
            resumeSignal.Set();

            // Give the unblocked thread time to execute its logic and fail.
            await Task.Delay(TimeSpan.FromSeconds(3));

            // Verify that the CompleteTaskActivityWorkItemAsync method was called exactly twice, and that the second call failed
            this.testService.OrchestrationServiceMock.Verify(
                svc => svc.CompleteTaskActivityWorkItemAsync(
                    It.IsAny<TaskActivityWorkItem>(),
                    It.IsAny<TaskMessage>()),
                Times.Exactly(2));

            // Check logs to confirm that a duplicate execution was detected
            LogAssert.Contains(
                this.testService.LogProvider,
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.CheckpointStarting(orchestrationName),
                LogAssert.CheckpointCompleted(orchestrationName),
                LogAssert.DuplicateExecutionDetected("SayHello"));
        }
    }
}
