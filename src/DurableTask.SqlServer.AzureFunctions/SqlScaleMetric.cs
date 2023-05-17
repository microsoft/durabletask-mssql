// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.WebJobs.Host.Scale;

    public class SqlScaleMetric : ScaleMetrics
    {
        public int RecommendedReplicaCount { get; set; }
    }
}
