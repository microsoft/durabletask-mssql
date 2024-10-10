﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System.Threading;
    using System.Threading.Tasks;

    public class SqlMetricsProvider
    {
        readonly SqlOrchestrationService service;

        public SqlMetricsProvider(SqlOrchestrationService service)
        {
            this.service = service;
        }

        public virtual async Task<SqlScaleMetric> GetMetricsAsync(int? previousWorkerCount = null)
        {
            // GetRecommendedReplicaCountAsync will write a trace if the recommendation results
            // in a worker count that is different from the worker count we pass in as an argument.
            int recommendedReplicaCount = await this.service.GetRecommendedReplicaCountAsync(
                previousWorkerCount,
                CancellationToken.None);

            return new SqlScaleMetric { RecommendedReplicaCount = recommendedReplicaCount };
        }
    }
}
