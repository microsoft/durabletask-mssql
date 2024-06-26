// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Host.Scale;

    /// <summary>
    /// Azure Functions scale monitor implementation for the Durable Functions SQL backend.
    /// </summary>
    class SqlScaleMonitor : IScaleMonitor<SqlScaleMetric>
    {
        static readonly ScaleStatus ScaleInVote = new ScaleStatus { Vote = ScaleVote.ScaleIn };
        static readonly ScaleStatus NoScaleVote = new ScaleStatus { Vote = ScaleVote.None };
        static readonly ScaleStatus ScaleOutVote = new ScaleStatus { Vote = ScaleVote.ScaleOut };

        readonly SqlOrchestrationService service;
        readonly SqlMetricsProvider metricsProvider;

        int? previousWorkerCount = -1;

        public SqlScaleMonitor(SqlOrchestrationService service, string taskHubName, SqlMetricsProvider sqlMetricsProvider)
        {
            this.service = service ?? throw new ArgumentNullException(nameof(service));
            this.Descriptor = new ScaleMonitorDescriptor($"DurableTask-SqlServer:{taskHubName ?? "default"}");
            this.metricsProvider = sqlMetricsProvider ?? throw new ArgumentNullException(nameof(sqlMetricsProvider));
        }

        /// <inheritdoc />
        public ScaleMonitorDescriptor Descriptor { get; }

        /// <inheritdoc />
        async Task<ScaleMetrics> IScaleMonitor.GetMetricsAsync() => await this.GetMetricsAsync();

        /// <inheritdoc />
        public async Task<SqlScaleMetric> GetMetricsAsync()
        {
            return await this.metricsProvider.GetMetricsAsync(this.previousWorkerCount);
        }

        /// <inheritdoc />
        ScaleStatus IScaleMonitor.GetScaleStatus(ScaleStatusContext context) => 
            this.GetScaleStatusCore(context.WorkerCount, context.Metrics.Cast<SqlScaleMetric>());

        /// <inheritdoc />
        public ScaleStatus GetScaleStatus(ScaleStatusContext<SqlScaleMetric> context) =>
            this.GetScaleStatusCore(context.WorkerCount, context.Metrics);

        ScaleStatus GetScaleStatusCore(int currentWorkerCount, IEnumerable<SqlScaleMetric> metrics)
        {
            SqlScaleMetric? mostRecentMetric = metrics.LastOrDefault();
            if (mostRecentMetric == null)
            {
                return NoScaleVote;
            }

            this.previousWorkerCount = currentWorkerCount;

            if (mostRecentMetric.RecommendedReplicaCount > currentWorkerCount)
            {
                return ScaleOutVote;
            }
            else if (mostRecentMetric.RecommendedReplicaCount < currentWorkerCount)
            {
                return ScaleInVote;
            }
            else
            {
                return NoScaleVote;
            }
        }
    }
}
