// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System.Threading.Tasks;
    using DurableTask.Core.Entities;
    using DurableTask.SqlServer.Tests.Utils;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class EntitySqlBackendQueriesTests : IAsyncLifetime
    {
        readonly TestService testService;
        readonly ITestOutputHelper output;

        public EntitySqlBackendQueriesTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
            this.output = output;
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(startWorker: false);

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task CleansMultiplePages()
        {
            const int EntityCount = 150;
            const string InstanceIdPrefix = "@CleanupPaging@";
            string taskHubName = await this.testService.GetTaskHubNameAsync();

            int insertedCount = (int)await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $@"
DECLARE @Entities TABLE (
    [InstanceID] varchar(100) NOT NULL,
    [PayloadID] uniqueidentifier NOT NULL,
    [CreatedTime] datetime2 NOT NULL);

WITH Numbers AS (
    SELECT 0 AS [Number]
    UNION ALL
    SELECT [Number] + 1 FROM Numbers WHERE [Number] + 1 < {EntityCount}
)
INSERT INTO @Entities
SELECT
    CONCAT('{InstanceIdPrefix}', FORMAT([Number], '000')),
    NEWID(),
    DATEADD(second, [Number], DATEADD(day, -1, SYSUTCDATETIME()))
FROM Numbers
OPTION (MAXRECURSION {EntityCount});

INSERT INTO dt.[Payloads] ([TaskHub], [InstanceID], [PayloadID], [Text])
SELECT '{taskHubName}', [InstanceID], [PayloadID], '{{""entityExists"":false}}'
FROM @Entities;

INSERT INTO dt.[Instances] (
    [TaskHub],
    [InstanceID],
    [ExecutionID],
    [Name],
    [Version],
    [CreatedTime],
    [LastUpdatedTime],
    [RuntimeStatus],
    [CustomStatusPayloadID])
SELECT
    '{taskHubName}',
    [InstanceID],
    CONVERT(varchar(50), NEWID()),
    'CleanupPaging',
    '',
    [CreatedTime],
    DATEADD(day, -1, SYSUTCDATETIME()),
    'Running',
    [PayloadID]
FROM @Entities;

SELECT COUNT(*) FROM @Entities;");
            Assert.Equal(EntityCount, insertedCount);

            EntityBackendQueries.CleanEntityStorageResult result =
                await this.testService.OrchestrationServiceMock.Object.EntityBackendQueries.CleanEntityStorageAsync(
                    new EntityBackendQueries.CleanEntityStorageRequest
                    {
                        RemoveEmptyEntities = true,
                    });

            Assert.Equal(EntityCount, result.EmptyEntitiesRemoved);
            Assert.Null(result.ContinuationToken);

            int remainingCount = (int)await SharedTestHelpers.ExecuteSqlAsync(
                this.output,
                $@"SELECT COUNT(*) FROM dt.[Instances]
WHERE [TaskHub] = '{taskHubName}' AND [InstanceID] LIKE '{InstanceIdPrefix}%';");
            Assert.Equal(0, remainingCount);
        }
    }
}
