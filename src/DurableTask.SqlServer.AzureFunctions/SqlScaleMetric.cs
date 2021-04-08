// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.WebJobs.Host.Scale;

    class SqlScaleMetric : ScaleMetrics
    {
        public int RecommendedReplicaCount { get; set; }
    }
}
