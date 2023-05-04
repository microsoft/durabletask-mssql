// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System.Threading;
    using System.Threading.Tasks;

    class SqlMetricsProvider
    {
        readonly SqlOrchestrationService service;
        readonly int? previousWorkerCount;

        public SqlMetricsProvider(SqlOrchestrationService service, int? previousWorkerCount = null)
        {
            this.service = service;
            this.previousWorkerCount = previousWorkerCount;
        }

        public async Task<SqlScaleMetric> GetMetricsAsync()
        {
            // GetRecommendedReplicaCountAsync will write a trace if the recommendation results
            // in a worker count that is different from the worker count we pass in as an argument.
            int recommendedReplicaCount = await this.service.GetRecommendedReplicaCountAsync(
                this.previousWorkerCount,
                CancellationToken.None);

            return new SqlScaleMetric { RecommendedReplicaCount = recommendedReplicaCount };
        }
    }
}
