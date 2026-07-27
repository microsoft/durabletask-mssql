// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class EntityQueueModes : IAsyncLifetime
    {
        readonly TestService testService;

        public EntityQueueModes(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(startWorker: false);

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task SwitchingToSeparateQueuesPartitionsExistingWork()
        {
            string orchestrationInstanceId = Guid.NewGuid().ToString("N");
            string entityInstanceId = $"@Counter@{Guid.NewGuid():N}";
            await this.EnqueueOrchestrationAndEntity(orchestrationInstanceId, entityInstanceId);

            this.testService.OrchestrationServiceOptions.UseSeparateQueueForEntityWorkItems = true;
            var entityService = (IEntityOrchestrationService)this.testService.OrchestrationServiceMock.Object;

            TaskOrchestrationWorkItem orchestration = await entityService.LockNextOrchestrationWorkItemAsync(
                TimeSpan.FromSeconds(5),
                CancellationToken.None);
            TaskOrchestrationWorkItem entity = await entityService.LockNextEntityWorkItemAsync(
                TimeSpan.FromSeconds(5),
                CancellationToken.None);

            Assert.Equal(orchestrationInstanceId, orchestration.InstanceId);
            Assert.False(Entities.IsEntityInstance(orchestration.InstanceId));
            Assert.Equal(entityInstanceId, entity.InstanceId);
            Assert.True(Entities.IsEntityInstance(entity.InstanceId));
        }

        [Fact]
        public async Task SwitchingToSharedQueueDrainsExistingEntityWork()
        {
            string entityInstanceId = $"@Counter@{Guid.NewGuid():N}";
            this.testService.OrchestrationServiceOptions.UseSeparateQueueForEntityWorkItems = true;
            await this.EnqueueOrchestrationAndEntity(Guid.NewGuid().ToString("N"), entityInstanceId);

            this.testService.OrchestrationServiceOptions.UseSeparateQueueForEntityWorkItems = false;
            TaskOrchestrationWorkItem firstWorkItem =
                await this.testService.OrchestrationServiceMock.Object.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromSeconds(5),
                    CancellationToken.None);
            TaskOrchestrationWorkItem secondWorkItem =
                await this.testService.OrchestrationServiceMock.Object.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromSeconds(5),
                    CancellationToken.None);

            Assert.Contains(
                entityInstanceId,
                new[] { firstWorkItem.InstanceId, secondWorkItem.InstanceId });
        }

        [Fact]
        public async Task SharedAndSeparateWorkersDoNotDoubleLockEntityWork()
        {
            string entityInstanceId = $"@Counter@{Guid.NewGuid():N}";
            await this.EnqueueOrchestrationAndEntity(Guid.NewGuid().ToString("N"), entityInstanceId);

            this.testService.OrchestrationServiceOptions.UseSeparateQueueForEntityWorkItems = true;
            var entityService = (IEntityOrchestrationService)this.testService.OrchestrationServiceMock.Object;

            Task<TaskOrchestrationWorkItem> sharedWorker = this.testService.OrchestrationServiceMock.Object
                .LockNextTaskOrchestrationWorkItemAsync(TimeSpan.FromSeconds(2), CancellationToken.None);
            Task<TaskOrchestrationWorkItem> separateEntityWorker = entityService.LockNextEntityWorkItemAsync(
                TimeSpan.FromSeconds(2),
                CancellationToken.None);
            Task<TaskOrchestrationWorkItem> separateOrchestrationWorker =
                entityService.LockNextOrchestrationWorkItemAsync(
                    TimeSpan.FromSeconds(2),
                    CancellationToken.None);

            await Task.WhenAll(sharedWorker, separateEntityWorker, separateOrchestrationWorker);

            TaskOrchestrationWorkItem[] lockedWorkItems =
            {
                sharedWorker.Result,
                separateEntityWorker.Result,
                separateOrchestrationWorker.Result,
            };
            Assert.Equal(2, lockedWorkItems.Count(item => item != null));
            Assert.Single(lockedWorkItems, item => item?.InstanceId == entityInstanceId);
        }

        Task<IReadOnlyList<TestInstance<string>>> EnqueueOrchestrationAndEntity(
            string orchestrationInstanceId,
            string entityInstanceId)
        {
            return this.testService.RunOrchestrations<string, string>(
                count: 2,
                instanceIdGenerator: i => i == 0 ? orchestrationInstanceId : entityInstanceId,
                inputGenerator: i => i.ToString(),
                orchestrationName: "QueueModeOrchestration",
                version: string.Empty,
                implementation: (context, input) => Task.FromResult(input));
        }
    }
}
