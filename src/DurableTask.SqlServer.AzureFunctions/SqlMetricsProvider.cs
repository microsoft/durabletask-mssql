// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class SqlMetricsProvider
    {
        readonly SqlOrchestrationService service;
        DateTime metricsTimeStamp = DateTime.MinValue;
        SqlScaleMetric? metrics;

        public SqlMetricsProvider(SqlOrchestrationService service)
        {
            this.service = service;
        }

        public virtual async Task<SqlScaleMetric> GetMetricsAsync(int? previousWorkerCount = null)
        {
            // We only want to query the metrics every 5 seconds.
            if (this.metrics == null || DateTime.UtcNow >= this.metricsTimeStamp.AddSeconds(5))
            {
                // GetRecommendedReplicaCountAsync will write a trace if the recommendation results
                // in a worker count that is different from the worker count we pass in as an argument.
                int recommendedReplicaCount = await this.service.GetRecommendedReplicaCountAsync(
                    previousWorkerCount,
                    CancellationToken.None);

                this.metricsTimeStamp = DateTime.UtcNow;
                this.metrics = new SqlScaleMetric { RecommendedReplicaCount = recommendedReplicaCount };
            }

            return this.metrics;
        }
    }
}
