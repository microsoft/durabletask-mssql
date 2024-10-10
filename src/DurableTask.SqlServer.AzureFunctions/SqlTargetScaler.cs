// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if FUNCTIONS_V4
namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Host.Scale;

    public class SqlTargetScaler : ITargetScaler
    {
        readonly SqlMetricsProvider sqlMetricsProvider;

        public SqlTargetScaler(string taskHubName, SqlMetricsProvider sqlMetricsProvider)
        {
            this.sqlMetricsProvider = sqlMetricsProvider;

            // Scalers in Durable Functions are shared for all functions in the same task hub.
            // So instead of using a function ID, we use the task hub name as the basis for the descriptor ID.
            string id = $"DurableTask-SqlServer:{taskHubName ?? "default"}";
            this.TargetScalerDescriptor = new TargetScalerDescriptor(id);
        }

        public TargetScalerDescriptor TargetScalerDescriptor { get; }

        public async Task<TargetScalerResult> GetScaleResultAsync(TargetScalerContext context)
        {
            SqlScaleMetric sqlScaleMetric = await this.sqlMetricsProvider.GetMetricsAsync();
            return new TargetScalerResult
            {
                TargetWorkerCount = Math.Max(0, sqlScaleMetric.RecommendedReplicaCount),
            };
        }
    }
}
#endif
