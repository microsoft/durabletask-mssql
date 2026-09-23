// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class ExtendedSessionEntityTests : IAsyncLifetime
    {
        readonly TestService testService;
        readonly ITestOutputHelper output;

        public ExtendedSessionEntityTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
            this.output = output;
        }

        // Extended sessions are enabled but the worker is not started: the tests drive the
        // lock/session lifecycle directly through the service so they can inspect an entity
        // work item before and after its lock is released, in each queue mode.
        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(
            startWorker: false,
            extendedSessions: true);

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Theory]
        [InlineData(false)] // entity work items share the orchestration queue
        [InlineData(true)]  // entity work items use a dedicated queue
        public async Task EntityWorkItemUnderSessionReleasesLock(bool useSeparateQueueForEntityWorkItems)
        {
            string entityInstanceId = $"@Counter@{Guid.NewGuid():N}";
            await this.EnqueueEntityAsync(entityInstanceId);

            this.testService.OrchestrationServiceOptions.UseSeparateQueueForEntityWorkItems =
                useSeparateQueueForEntityWorkItems;

            SqlOrchestrationService service = this.testService.OrchestrationServiceMock.Object;
            var entityService = (IEntityOrchestrationService)service;

            // In separate-queue mode entities are only returned by LockNextEntityWorkItemAsync; in
            // shared-queue mode they flow through the normal orchestration work item path.
            TaskOrchestrationWorkItem workItem = useSeparateQueueForEntityWorkItems
                ? await entityService.LockNextEntityWorkItemAsync(TimeSpan.FromSeconds(10), CancellationToken.None)
                : await service.LockNextTaskOrchestrationWorkItemAsync(TimeSpan.FromSeconds(10), CancellationToken.None);

            Assert.NotNull(workItem);
            Assert.Equal(entityInstanceId, workItem.InstanceId);
            Assert.True(Entities.IsEntityInstance(workItem.InstanceId));

            // With extended sessions enabled, the entity work item carries a session that owns the
            // SQL lock — the same mechanism used for orchestrations, in both queue modes.
            Assert.NotNull(workItem.Session);

            // The lock is held under a unique per-acquisition token while the session is active.
            string lockedBy = await this.GetLockedByAsync(entityInstanceId);
            Assert.False(string.IsNullOrEmpty(lockedBy));

            // Releasing the work item (what the dispatcher does when the session ends) must clear
            // the entity's lock so another worker can pick it up.
            await service.ReleaseTaskOrchestrationWorkItemAsync(workItem);

            Assert.Equal(string.Empty, await this.GetLockedByAsync(entityInstanceId));
        }

        Task EnqueueEntityAsync(string entityInstanceId)
        {
            // Creating an instance with an entity-formatted ID enqueues an entity work item without
            // requiring the worker to run. The implementation is never invoked (startWorker: false);
            // we only need the instance and its pending message to exist so LockNext can find it.
            return this.testService.RunOrchestrations<string, string>(
                count: 1,
                instanceIdGenerator: _ => entityInstanceId,
                inputGenerator: _ => "0",
                orchestrationName: nameof(ExtendedSessionEntityTests),
                version: string.Empty,
                implementation: (context, input) => Task.FromResult(input));
        }

        async Task<string> GetLockedByAsync(string instanceId)
        {
            object result = await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $"SELECT TOP 1 ISNULL([LockedBy], '') FROM dt.[Instances] WHERE [InstanceID] = '{instanceId}'");
            return result?.ToString() ?? string.Empty;
        }
    }
}
