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

        public SqlTargetScaler(string functionId, SqlMetricsProvider sqlMetricsProvider)
        {
            this.sqlMetricsProvider = sqlMetricsProvider;

            // Scalers in Durable Functions is per function ids. And scalers share the same sqlMetricsProvider in the same taskhub. 
            this.TargetScalerDescriptor = new TargetScalerDescriptor(functionId);
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
