// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Utils;
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
        public async Task LockNotPoachedWhileSessionActive()
        {
            TaskCompletionSource<string> tcs = null;

            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                input: null,
                orchestrationName: nameof(LockNotPoachedWhileSessionActive),
                implementation: (ctx, _) =>
                {
                    tcs = new TaskCompletionSource<string>();
                    return tcs.Task;
                },
                onEvent: (ctx, name, value) => tcs.TrySetResult(JsonConvert.DeserializeObject<string>(value)));

            await instance.WaitForStart();
            string heldBy = await this.WaitForLockToBeHeldAsync(instance.InstanceId, TimeSpan.FromSeconds(10));

            // Sanity check: a second worker calling _LockNextOrchestration sees no available work
            // because the only ready instance is locked by the in-flight session.
            object available = await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $"SELECT COUNT(*) FROM dt.[Instances] WHERE [InstanceID] = '{instance.InstanceId}' AND [LockedBy] = '{heldBy}' AND [LockExpiration] > SYSUTCDATETIME()");
            Assert.Equal(1, Convert.ToInt32(available));

            await instance.RaiseEventAsync("Continue", "done");
            await instance.WaitForCompletion(expectedOutput: "done");
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
