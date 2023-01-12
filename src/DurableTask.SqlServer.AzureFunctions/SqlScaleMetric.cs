// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.WebJobs.Host.Scale;

    /// <summary>
    /// Contains metrics used to make scale decisions for a SqlScaleMetric.
    /// </summary>
    public class SqlScaleMetric : ScaleMetrics
    {
        public int RecommendedReplicaCount { get; set; }
    }
}
