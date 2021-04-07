// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Logging;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;

    public class ScaleTests : IAsyncLifetime
    {
        readonly TestService testService;

        public ScaleTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(startWorker: false);

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        /// <summary>
        /// Validates the scale recommendation behavior when scheduling new orchestration instances.
        /// </summary>
        [Fact]
        public async Task ScaleRecommendation_PendingOrchestrationStarts()
        {
            // Block the orchestration dispatch loop so that we can queue orchestrations without running them
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskOrchestrationWorkItemAsync(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns(() => Task.Delay(100).ContinueWith(t => default(TaskOrchestrationWorkItem)));

            // We can influence the recommended replica count by modifying concurrency settings
            this.testService.OrchestrationServiceOptions.MaxActiveOrchestrations = 1;
            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            for (int i = 1; i <= 10; i++)
            {
                // Schedule an orchestration (it won't run, see above)
                await this.testService.RunOrchestration(
                    null as string,
                    $"EmptyOrchestration{i:00}",
                    implementation: (ctx, input) => Task.FromResult(input));

                // Scale recommendation increments with every new scheduled orchestration.
                // We pass in the previous recommendation to generate a log statement about the change.
                recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(recommendation);
                Assert.Equal(i, recommendation);
            }

            // Validate the logs
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            Assert.Equal(10, recommendationChangeLogs.Count);

            for (int i = 0; i < recommendationChangeLogs.Count; i++)
            {
                LogEntry log = recommendationChangeLogs[i];
                LogAssert.FieldEquals(log, "CurrentCount", i);
                LogAssert.FieldEquals(log, "RecommendedCount", i + 1);
            }
        }

        /// <summary>
        /// Validates the scale recommendation behavior when scheduling activities.
        /// </summary>
        /// <param name="activityCount">The number of activities to schedule.</param>
        /// <param name="expectedRecommendation">The expected scale recommendation.</param>
        [Theory]
        [InlineData(01, 01)]
        [InlineData(10, 04)]
        [InlineData(99, 33)]
        public async Task ScaleRecommendation_PendingActivities(int activityCount, int expectedRecommendation)
        {
            // Block the activity dispatch loop so that we can queue activities without running them
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskActivityWorkItem(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns(() => Task.Delay(100).ContinueWith(t => default(TaskActivityWorkItem)));

            // We can influence the recommended replica count by modifying concurrency settings
            this.testService.OrchestrationServiceOptions.MaxConcurrentActivities = 3;
            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            TestInstance<string> instance = await this.testService.RunOrchestration<string[], string>(
                null,
                orchestrationName: "OrchestrationWithActivityFanOut",
                implementation: (ctx, _) =>
                {
                    return Task.WhenAll(Enumerable.Range(0, activityCount).Select(i => ctx.ScheduleTask<string>("ToString", "", i)).ToArray());
                },
                activities: new[] {
                    ("ToString", TestService.MakeActivity((TaskContext ctx, int input) => input.ToString())),
                });

            // All activities should be scheduled when the instances transitions into the Running state.
            // Also, the orchestration will be unloaded and not count towards the recommendation.
            await instance.WaitForStart();

            // The next recommendation should suggest 1 worker for every 3 pending activities
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: 0);
            Assert.Equal(expectedRecommendation, recommendation);

            // Validate the logs
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            LogEntry log = Assert.Single(recommendationChangeLogs);
            LogAssert.FieldEquals(log, "CurrentCount", 0);
            LogAssert.FieldEquals(log, "RecommendedCount", expectedRecommendation);
        }

        /// <summary>
        /// Validates the scale recommendation behavior when scheduling timers.
        /// </summary>
        /// <param name="expectedRecommendation">The number of concurrent orchestrations waiting on timers.</param>
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task ScaleRecommendation_PendingTimers(int expectedRecommendation)
        {
            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations<string, string>(
                expectedRecommendation,
                instanceIdGenerator: null,
                inputGenerator: null,
                orchestrationName: "SingleTimer",
                version: null,
                implementation: (ctx, _) =>
                {
                    // Schedule into the distant future
                    return ctx.CreateTimer(ctx.CurrentUtcDateTime.AddDays(1), "done!");
                });

            // All timers should be scheduled when the instances transitions into the Running state.
            // Also, the orchestration will be unloaded and not count towards the recommendation.
            await Task.WhenAll(instances.Select(i => i.WaitForStart()));

            // The next recommendation should suggest 0 workers since all pending events are just distant timers
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: 0);
            Assert.Equal(0, recommendation);

            // Validate the logs (there should be none)
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            Assert.Equal(0, recommendationChangeLogs.Count);
        }

        /// <summary>
        /// Validates the scale recommendation behavior when timers have expired.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task ScaleRecommendation_ExpiredTimers(int timerCount)
        {
            const int MaxOrchestrationConcurrency = 5;
            int expectedRecommendation = (int)Math.Ceiling(timerCount / (double)MaxOrchestrationConcurrency);

            // Later in the test we'll block orchestrations from starting to simulate a scaled-to-zero app.
            var orchestrationExecutionGate = new SemaphoreSlim(initialCount: 0, maxCount: timerCount);
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskOrchestrationWorkItemAsync(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Callback(() => orchestrationExecutionGate.Wait());

            this.testService.OrchestrationServiceOptions.MaxActiveOrchestrations = MaxOrchestrationConcurrency;
            this.testService.OrchestrationServiceOptions.WorkItemBatchSize = timerCount;

            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            TimeSpan delay = TimeSpan.FromSeconds(3);

            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations<string, string>(
                timerCount,
                instanceIdGenerator: null,
                inputGenerator: null,
                orchestrationName: "SingleTimer",
                version: null,
                implementation: (ctx, _) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), "done!"));

            // Allow the start messages to get processed. Each unblocked thread will process exactly one orchestration start.
            // After that, this semaphore gate will expire blocking subsequent orchestration dispatch, helping us ensure
            // that expired timers don't get processed before we have a chance to run the test logic below.
            orchestrationExecutionGate.Release(timerCount);

            // All timers should be scheduled when the instances transitions into the Running state.
            // Also, the orchestration will be unloaded and not count towards the recommendation.
            await Task.WhenAll(instances.Select(i => i.WaitForStart()));

            // Now delay so that the timers can expire and have their expiration events added to the orchestration queue.
            await Task.Delay(delay + TimeSpan.FromMilliseconds(500));

            // The next recommendation should suggest N workers since all pending events are expired timers
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: 0);
            Assert.Equal(expectedRecommendation, recommendation);

            // Validate the logs (there should be one)
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            LogEntry log = Assert.Single(recommendationChangeLogs);
            LogAssert.FieldEquals(log, "CurrentCount", 0);
            LogAssert.FieldEquals(log, "RecommendedCount", expectedRecommendation);
        }

        /// <summary>
        /// Validates the scale recommendation behavior when activities are actively executing.
        /// </summary>
        [Fact]
        public async Task ScaleRecommendation_ActiveActivities()
        {
            // We can influence the recommended replica count by modifying concurrency settings
            this.testService.OrchestrationServiceOptions.MaxConcurrentActivities = 1;
            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            using var activityStartedGate = new ManualResetEventSlim(initialState: false);
            using var activityCompleteGate = new ManualResetEventSlim(initialState: false);

            TestInstance<string> instance = await this.testService.RunOrchestration<bool, string>(
                null,
                orchestrationName: "SingleActivity",
                implementation: (ctx, _) =>
                {
                    return ctx.ScheduleTask<bool>("WaitForSignal", "", 30);
                },
                activities: new[]
                {
                    ("WaitForSignal", TestService.MakeActivity(
                        (TaskContext ctx, int input) =>
                        {
                            activityStartedGate.Set();
                            return activityCompleteGate.Wait(TimeSpan.FromSeconds(input));
                        })),
                });

            // Wait for all activities to start
            Assert.True(activityStartedGate.Wait(TimeSpan.FromSeconds(30).AdjustForDebugging()));

            // The next recommendation should suggest 1 worker for every active activity
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: 0);
            Assert.Equal(1, recommendation);

            // Allow the activities to complete
            activityCompleteGate.Set();
            await instance.WaitForCompletion(expectedOutput: true);

            // Recommendation should return back to zero now that everything is complete
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: recommendation);
            Assert.Equal(0, recommendation);

            // Validate the logs
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            Assert.Equal(2, recommendationChangeLogs.Count);

            // Initial scale-out to N
            LogAssert.FieldEquals(recommendationChangeLogs[0], "CurrentCount", 0);
            LogAssert.FieldEquals(recommendationChangeLogs[0], "RecommendedCount", 1);

            // Scale-in back to zero
            LogAssert.FieldEquals(recommendationChangeLogs[1], "CurrentCount", 1);
            LogAssert.FieldEquals(recommendationChangeLogs[1], "RecommendedCount", 0);
        }

        /// <summary>
        /// Validates the scale recommendation calculation when a single orchestration has
        /// multiple events pending (only one worker should be recommended).
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task ScaleRecommendation_BackloggedOrchestration(int orchestrationCount)
        {
            const int ActivityCount = 10;
            int totalActivities = orchestrationCount * ActivityCount;

            // Let N orchestrations start but prevent them from responding to activity results
            using var orchestrationExecutionGate = new SemaphoreSlim(initialCount: orchestrationCount);
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskOrchestrationWorkItemAsync(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Callback(() => orchestrationExecutionGate.Wait());

            // Block activities from starting until all orchestrations have had a chance to run
            using var activityExecutionGate = new ManualResetEventSlim(initialState: false);
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskActivityWorkItem(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Callback(() => activityExecutionGate.Wait());

            // Signal the test to resume once all the activities finish executing
            using var activityCompletionCountdown = new CountdownEvent(initialCount: totalActivities);
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.CompleteTaskActivityWorkItemAsync(
                    It.IsAny<TaskActivityWorkItem>(),
                    It.IsAny<TaskMessage>()))
                .CallBase()
                .Callback(() => activityCompletionCountdown.Signal()); // NOTE: This callback happens mid-execution of CompleteTaskActivityWorkItemAsync

            this.testService.OrchestrationServiceOptions.MaxActiveOrchestrations = 1;
            this.testService.OrchestrationServiceOptions.MaxConcurrentActivities = totalActivities;

            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            using var activityCountdown = new CountdownEvent(initialCount: totalActivities);

            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations<string[], string>(
                orchestrationCount,
                instanceIdGenerator: null,
                inputGenerator: null,
                orchestrationName: "OrchestrationWithActivityFanOut",
                version: null,
                implementation: (ctx, _) =>
                {
                    return Task.WhenAll(Enumerable.Range(0, ActivityCount).Select(i => ctx.ScheduleTask<string>("ToString", "", i)).ToArray());
                },
                activities: new[]
                {
                    ("ToString", TestService.MakeActivity((TaskContext ctx, int input) => input.ToString())),
                });

            await Task.WhenAll(instances.Select(i => i.WaitForStart()));

            // Unblock the activities now that the orchestrations have all started
            activityExecutionGate.Set();

            // Wait for all activities to complete and enqueue their responses
            Assert.True(activityCompletionCountdown.Wait(TimeSpan.FromSeconds(10).AdjustForDebugging()));

            // Need to wait a little longer since not all database transactions will have completed yet
            await Task.Delay(TimeSpan.FromSeconds(1));

            // The next recommendation should suggest 1 worker for each pending orchestration, regardless
            // of how many activity responses there are.
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: 0);
            Assert.Equal(orchestrationCount, recommendation);

            // Validate the logs (there should be one)
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            LogEntry log = Assert.Single(recommendationChangeLogs);
            LogAssert.FieldEquals(log, "CurrentCount", 0);
            LogAssert.FieldEquals(log, "RecommendedCount", orchestrationCount);
        }

        [Theory]
        [InlineData(1, 1, 1, 1, 2)]
        [InlineData(1, 10, 100, 1, 11)]
        [InlineData(5, 10, 100, 1, 11)]
        public async Task ScaleRecommendation_MixedLoad(
            int pendingOrchestrations,
            int pendingActivities,
            int maxPendingOrchestrations,
            int maxPendingActivities,
            int expectedRecommendation)
        {
            // Allow just one orchestration to start and schedule all the activities. Subsequent orchestration
            // executions will be blocked so we can acrue pending orchestration counts.
            var orchestrationExecutionGate = new SemaphoreSlim(initialCount: 1);
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskOrchestrationWorkItemAsync(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Callback(() => orchestrationExecutionGate.Wait());

            // Block the activity dispatch loop so that we can queue activities without running them.
            this.testService.OrchestrationServiceMock.Setup(
                svc => svc.LockNextTaskActivityWorkItem(
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns(() => Task.Delay(100).ContinueWith(t => default(TaskActivityWorkItem)));

            this.testService.OrchestrationServiceOptions.MaxActiveOrchestrations = maxPendingOrchestrations;
            this.testService.OrchestrationServiceOptions.MaxConcurrentActivities = maxPendingActivities;

            await this.testService.StartWorkerAsync();

            SqlOrchestrationService orchestrationService = this.testService.OrchestrationServiceMock.Object;

            // Initial scale recommendation should be zero
            int recommendation = await orchestrationService.GetRecommendedReplicaCountAsync();
            Assert.Equal(0, recommendation);

            // This one orchestration is responsible for scheduling all the activities. It then goes idle indefinitely.
            TestInstance<string> instance = await this.testService.RunOrchestration<string[], string>(
                null,
                orchestrationName: "OrchestrationWithActivityFanOut",
                implementation: (ctx, _) =>
                {
                    return Task.WhenAll(Enumerable.Range(0, pendingActivities).Select(i => ctx.ScheduleTask<string>("ToString", "", i)).ToArray());
                },
                activities: new[]
                {
                    ("ToString", TestService.MakeActivity((TaskContext ctx, int input) => input.ToString())),
                });

            // We know all the activities are scheduled once the instance is in a "Running" state.
            await instance.WaitForStart();

            // Finally, schedule all the pending orchestrations, as simple no-ops. None of them should actually start running and
            // should therefore be included in our scale recommendation calculation.
            IReadOnlyList<TestInstance<string>> instances = await this.testService.RunOrchestrations<string, string>(
                pendingOrchestrations,
                instanceIdGenerator: null,
                inputGenerator: null,
                orchestrationName: "NoOp",
                version: null,
                implementation: (ctx, _) => Task.FromResult("done!"));

            // Verify final scale recommendation
            recommendation = await orchestrationService.GetRecommendedReplicaCountAsync(currentReplicaCount: recommendation);
            Assert.Equal(expectedRecommendation, recommendation);

            // Validate the logs (there should be one)
            IReadOnlyList<LogEntry> recommendationChangeLogs = this.GetRecommendationChangeLogs();
            LogEntry log = Assert.Single(recommendationChangeLogs);
            LogAssert.FieldEquals(log, "CurrentCount", 0);
            LogAssert.FieldEquals(log, "RecommendedCount", expectedRecommendation);
        }

        IReadOnlyList<LogEntry> GetRecommendationChangeLogs() => this.testService.GetAndValidateLogs()
            .Where(log => log.EventId == EventIds.ReplicaCountChangeRecommended)
            .ToList();
    }
}
