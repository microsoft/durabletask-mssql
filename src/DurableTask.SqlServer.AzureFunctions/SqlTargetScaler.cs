// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Host.Scale;

#if NETCOREAPP
    class SqlTargetScaler : ITargetScaler
    {
        readonly SqlMetricsProvider sqlMetricsProvider;
        readonly TargetScalerResult scaleResult;

        public SqlTargetScaler(string functionId, SqlMetricsProvider sqlMetricsProvider)
        {
            this.sqlMetricsProvider = sqlMetricsProvider;
            this.scaleResult = new TargetScalerResult();
            this.TargetScalerDescriptor = new TargetScalerDescriptor(functionId);
        }

        public TargetScalerDescriptor TargetScalerDescriptor { get; private set; }

        public async Task<TargetScalerResult> GetScaleResultAsync(TargetScalerContext context)
        {
            SqlScaleMetric sqlScaleMetric = await this.sqlMetricsProvider.GetMetricsAsync();
            this.scaleResult.TargetWorkerCount = sqlScaleMetric.RecommendedReplicaCount;
            return this.scaleResult;
        }
    }
#endif
}
