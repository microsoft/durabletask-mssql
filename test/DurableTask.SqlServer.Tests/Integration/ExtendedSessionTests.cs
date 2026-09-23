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
    using DurableTask.SqlServer.Tests.Utils;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class ExtendedSessionTests : IAsyncLifetime
    {
        readonly TestService testService;
        readonly ITestOutputHelper output;

        public ExtendedSessionTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
            this.output = output;
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(
            extendedSessions: true,
            extendedSessionIdleTimeout: TimeSpan.FromSeconds(15));

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task LockHeldWhileWaitingForExternalEvent()
        {
            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockHeldWhileWaitingForExternalEvent),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            await instance.RaiseEventAsync("Continue", "done");
            await instance.WaitForCompletion(expectedOutput: "done");

            string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
            Assert.Equal(string.Empty, lockedBy);
        }

        [Fact]
        public async Task LockReleasedAfterIdleTimeout()
        {
            this.testService.OrchestrationServiceOptions.ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(2);

            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockReleasedAfterIdleTimeout),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));
            await this.WaitForLockToBeReleasedAsync(instance.InstanceId, TimeSpan.FromSeconds(15));

            // The orchestration should still be Running — only the lock has expired/cleared.
            OrchestrationState midState = await instance.GetStateAsync();
            Assert.Equal(OrchestrationStatus.Running, midState.OrchestrationStatus);

            // Re-engaging the orchestration should still work end-to-end after the session ended.
            await instance.RaiseEventAsync("Continue", "done");
            await instance.WaitForCompletion(expectedOutput: "done");
        }

        [Fact]
        public async Task LockReleasedOnWorkerShutdown()
        {
            // Use a long idle timeout so the idle-timeout path cannot be what clears the lock —
            // any release must be attributable to the worker shutting down.
            this.testService.OrchestrationServiceOptions.ExtendedSessionIdleTimeout = TimeSpan.FromMinutes(5);

            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockReleasedOnWorkerShutdown),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            // Gracefully drain the worker while the session is still holding the lock. This must not
            // throw, and the session must unwind so the lock no longer dangles.
            Exception shutdownException = await Record.ExceptionAsync(
                () => this.testService.StopWorkerAsync(isForced: false));
            Assert.Null(shutdownException);

            await this.WaitForLockToBeReleasedAsync(instance.InstanceId, TimeSpan.FromSeconds(30));
        }

        [Fact]
        public async Task LockNotPoachedWhileSessionActive()
        {
            // A locked-but-uncheckpointed work item still has its NewEvents rows, which is the only
            // state where _LockNextOrchestration would hand out the instance if the lease were ignored.
            await this.testService.StopWorkerAsync(isForced: true);

            // Stopping the worker cancelled the service's shutdown token; re-arm it.
            SqlOrchestrationService firstWorker = this.testService.OrchestrationServiceMock.Object;
            await firstWorker.StartAsync();

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockNotPoachedWhileSessionActive),
                implementation: (ctx, _) => Task.FromResult("done"));

            TaskOrchestrationWorkItem firstWorkItem = await firstWorker.LockNextTaskOrchestrationWorkItemAsync(
                TimeSpan.FromSeconds(10).AdjustForDebugging(),
                CancellationToken.None);

            Assert.NotNull(firstWorkItem);
            Assert.Equal(instance.InstanceId, firstWorkItem.InstanceId);

            // Same low-privilege credentials: dt.CurrentTaskHub() is derived from the login.
            var secondWorkerSettings = new SqlOrchestrationServiceSettings(
                this.testService.TestCredentialConnectionString)
            {
                AppName = "second-worker",
                ExtendedSessionsEnabled = true,
                LoggerFactory = LoggerFactory.Create(
                    builder => builder.AddProvider(this.testService.LogProvider)),
            };

            var secondWorker = new SqlOrchestrationService(secondWorkerSettings);
            await secondWorker.StartAsync();

            TaskOrchestrationWorkItem poachedWorkItem = null;
            try
            {
                Assert.Null(await secondWorker.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromSeconds(2).AdjustForDebugging(),
                    CancellationToken.None));

                // Positive control: the same call succeeds once the lease is handed back.
                await firstWorker.ReleaseTaskOrchestrationWorkItemAsync(firstWorkItem);

                poachedWorkItem = await secondWorker.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromSeconds(10).AdjustForDebugging(),
                    CancellationToken.None);

                Assert.NotNull(poachedWorkItem);
                Assert.Equal(instance.InstanceId, poachedWorkItem.InstanceId);

                string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
                Assert.StartsWith("second-worker,", lockedBy);
            }
            finally
            {
                if (poachedWorkItem != null)
                {
                    await secondWorker.ReleaseTaskOrchestrationWorkItemAsync(poachedWorkItem);
                }

                await secondWorker.StopAsync();
            }
        }

        [Fact]
        public async Task MultipleEventsAcrossSession()
        {
            const int eventCount = 5;
            TaskCompletionSource<int> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<int, string>(
                input: null,
                orchestrationName: nameof(MultipleEventsAcrossSession),
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
                    tcs.TrySetResult(int.Parse(value));
                });

            for (int i = 0; i < eventCount; i++)
            {
                await instance.RaiseEventAsync($"Event{i}", i);
            }

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: eventCount);

            string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
            Assert.Equal(string.Empty, lockedBy);
        }

        [Fact]
        public async Task EventsDeliveredInOrderAcrossSession()
        {
            const int eventCount = 10;
            TaskCompletionSource<int> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(EventsDeliveredInOrderAcrossSession),
                implementation: async (ctx, _) =>
                {
                    // 'received' is rebuilt from awaited results on every replay, so it stays
                    // deterministic. Its contents reflect the order events were delivered to the
                    // orchestrator, which mirrors the order _FetchOrchestrationMessages returned them.
                    var received = new List<int>();
                    for (int i = 0; i < eventCount; i++)
                    {
                        tcs = new TaskCompletionSource<int>();
                        received.Add(await tcs.Task);
                    }

                    return string.Join(",", received);
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(int.Parse(value)));

            await instance.WaitForStart();
            await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            // Raise events in a known order. They accumulate in NewEvents and are fetched as a
            // batch by the extended session; without a deterministic ORDER BY on SequenceNumber
            // they could be delivered — and therefore observed — out of order.
            for (int i = 0; i < eventCount; i++)
            {
                await instance.RaiseEventAsync("Number", i);
            }

            string expected = string.Join(",", Enumerable.Range(0, eventCount));
            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(20),
                expectedOutput: expected);

            string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
            Assert.Equal(string.Empty, lockedBy);
        }

        [Fact]
        public async Task ContinueAsNewWithSession()
        {
            TestInstance<int> instance = await this.testService.RunOrchestration(
                input: 0,
                orchestrationName: nameof(ContinueAsNewWithSession),
                implementation: async (ctx, input) =>
                {
                    if (input < 3)
                    {
                        await ctx.CreateTimer<object>(DateTime.MinValue, null);
                        ctx.ContinueAsNew(input + 1);
                    }

                    return input;
                });

            await instance.WaitForCompletion(expectedOutput: 3, continuedAsNew: true);

            string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
            Assert.Equal(string.Empty, lockedBy);
        }

        [Fact]
        public async Task ContinueAsNewWithScheduledWorkAcrossSession()
        {
            const int generations = 3;

            // The activity (task ID 0) and sub-orchestration (task ID 1) are re-scheduled in every
            // generation, so their IDs restart after each ContinueAsNew. In a retained session the
            // EventPayloadMap is reused across generations, so without clearing it after each
            // checkpoint the second generation's task ID 0 would collide with the first generation's
            // key and throw before the checkpoint.
            this.testService.RegisterInlineActivity(
                "CanEcho", string.Empty, TestService.MakeActivity<int, int>((ctx, input) => input));
            this.testService.RegisterInlineOrchestration<int, int>(
                "CanChild", string.Empty, implementation: (ctx, input) => Task.FromResult(input));

            TestInstance<int> instance = await this.testService.RunOrchestration(
                input: 0,
                orchestrationName: nameof(ContinueAsNewWithScheduledWorkAcrossSession),
                implementation: async (ctx, input) =>
                {
                    int fromActivity = await ctx.ScheduleTask<int>("CanEcho", string.Empty, input);
                    int fromChild = await ctx.CreateSubOrchestrationInstance<int>("CanChild", string.Empty, input);

                    if (input < generations)
                    {
                        ctx.ContinueAsNew(input + 1);
                    }

                    return fromActivity + fromChild;
                });

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(30),
                expectedOutput: generations * 2,
                continuedAsNew: true);

            string lockedBy = await this.GetLockedByAsync(instance.InstanceId);
            Assert.Equal(string.Empty, lockedBy);

            // The orchestration self-heals from a payload-map collision by aborting and re-locking
            // with a fresh map, so completion alone does not prove correctness. Assert the checkpoint
            // never threw the collision (which the dispatcher logs against the DurableTask.Core category).
            this.testService.LogProvider.TryGetLogs("DurableTask.Core", out var coreLogs);
            Assert.DoesNotContain(
                coreLogs,
                entry => entry.Exception != null && entry.Exception.ToString().Contains("EventPayloadMap"));
        }

        [Fact]
        public async Task LockLostRecovers()
        {
            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockLostRecovers),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            // Forcibly clear the lock
            await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $"UPDATE dt.[Instances] SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [InstanceID] = '{instance.InstanceId}'");

            await instance.RaiseEventAsync("Continue", "done");
            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(20),
                expectedOutput: "done");

            // Completion alone doesn't prove the session noticed: the lost lock must surface as a
            // SessionAbortedException, which the dispatcher logs under DurableTask.Core.
            this.testService.LogProvider.TryGetLogs("DurableTask.Core", out var coreLogs);
            Assert.Contains(
                coreLogs,
                entry => entry.Message != null &&
                    entry.Message.Contains($"Lost the lock for instance '{instance.InstanceId}'"));
        }

        [Fact]
        public async Task StaleLeaseOperationsAreRejected()
        {
            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(StaleLeaseOperationsAreRejected),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            string liveToken = await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            const string staleToken = "stale-owner-token";
            const string future = "2999-01-01T00:00:00";
            string id = instance.InstanceId;
            string execId = instance.ExecutionId;

            // A stale owner token — e.g. a session whose lease expired and was reacquired by another
            // acquisition — must be rejected by every lock-guarded stored procedure with error 50003.
            Assert.Equal(50003, await this.RunGuardedProcAsync(
                $"EXEC dt._FetchOrchestrationMessages @InstanceID='{id}', @LockedBy='{staleToken}', @LockExpiration='{future}', @BatchSize=10;"));

            Assert.Equal(50003, await this.RunGuardedProcAsync(
                $"EXEC dt._RenewOrchestrationLocks @InstanceID='{id}', @LockExpiration='{future}', @LockedBy='{staleToken}';"));

            Assert.Equal(50003, await this.RunGuardedProcAsync($@"
DECLARE @de dt.MessageIDs;
DECLARE @he dt.HistoryEvents;
DECLARE @oe dt.OrchestrationEvents;
DECLARE @te dt.TaskEvents;
EXEC dt._CheckpointOrchestration
    @InstanceID='{id}', @ExecutionID='{execId}', @RuntimeStatus='Running',
    @CustomStatusPayload=NULL, @DeletedEvents=@de, @NewHistoryEvents=@he,
    @NewOrchestrationEvents=@oe, @NewTaskEvents=@te,
    @KeepLocked=1, @LockedBy='{staleToken}', @NewLockExpiration='{future}';"));

            // Release with a stale token must not clear the real owner's lock.
            await this.RunGuardedProcAsync(
                $"EXEC dt._ReleaseOrchestrationLock @InstanceID='{id}', @LockedBy='{staleToken}';");
            Assert.Equal(liveToken, await this.GetLockedByAsync(id));

            // The guard is genuine, not always-failing: the real owner can renew its unexpired lease.
            Assert.Equal(0, await this.RunGuardedProcAsync(
                $"EXEC dt._RenewOrchestrationLocks @InstanceID='{id}', @LockExpiration='{future}', @LockedBy='{liveToken}';"));

            // Even the correct owner is rejected once the lease has expired.
            await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $"UPDATE dt.[Instances] SET [LockExpiration] = DATEADD(MINUTE, -5, SYSUTCDATETIME()) WHERE [InstanceID] = '{id}'");
            Assert.Equal(50003, await this.RunGuardedProcAsync(
                $"EXEC dt._RenewOrchestrationLocks @InstanceID='{id}', @LockExpiration='{future}', @LockedBy='{liveToken}';"));

            // The orchestration still drives to completion after the lease is lost: the stale session
            // aborts and a fresh lock acquisition takes over.
            await instance.RaiseEventAsync("Continue", "done");
            await instance.WaitForCompletion(timeout: TimeSpan.FromSeconds(30), expectedOutput: "done");

            this.testService.LogProvider.TryGetLogs("DurableTask.Core", out var coreLogs);
            Assert.Contains(
                coreLogs,
                entry => entry.Message != null &&
                    entry.Message.Contains($"Lost the lock for instance '{id}'"));
        }

        async Task<int> RunGuardedProcAsync(string procBatch)
        {
            string sql = $@"
BEGIN TRY
    {procBatch}
    SELECT 0;
END TRY
BEGIN CATCH
    SELECT ERROR_NUMBER();
END CATCH";
            object result = await SharedTestHelpers.ExecuteSqlAsync(
                this.output, sql, this.testService.TestCredentialConnectionString);
            return Convert.ToInt32(result);
        }

        async Task<string> GetLockedByAsync(string instanceId)
        {
            object result = await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $"SELECT TOP 1 ISNULL([LockedBy], '') FROM dt.[Instances] WHERE [InstanceID] = '{instanceId}'");
            return result?.ToString() ?? string.Empty;
        }

        async Task<string> WaitForLockToBeHeldAsync(string instanceId, TimeSpan timeout)
        {
            timeout = timeout.AdjustForDebugging();
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                string lockedBy = await this.GetLockedByAsync(instanceId);
                if (!string.IsNullOrEmpty(lockedBy))
                {
                    return lockedBy;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }

            throw new TimeoutException(
                $"Instance '{instanceId}' lock was not held within {timeout.TotalSeconds}s — extended session did not engage.");
        }

        async Task WaitForLockToBeReleasedAsync(string instanceId, TimeSpan timeout)
        {
            timeout = timeout.AdjustForDebugging();
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                string lockedBy = await this.GetLockedByAsync(instanceId);
                if (string.IsNullOrEmpty(lockedBy))
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }

            throw new TimeoutException(
                $"Instance '{instanceId}' lock was not released within {timeout.TotalSeconds}s.");
        }
    }
}
