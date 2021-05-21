// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;

    public static class PurgeOrchestrationData
    {
        [FunctionName("PurgeOrchestrationData")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            log.LogWarning("Purging all orchestration data from the database");

            int totalDeleted = 0;
            bool finished = false;

            // Stop after 25 seconds to avoid a client-side timeout
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(25))
            {
                // Purge all completed instances, dating back to the year 2000
                PurgeHistoryResult result = await client.PurgeInstanceHistoryAsync(
                    createdTimeFrom: new DateTime(2000, 1, 1),
                    createdTimeTo: null,
                    runtimeStatus: null);

                totalDeleted += result.InstancesDeleted;

                // The SQL provider only deletes at most 1000 instances per call
                if (result.InstancesDeleted < 1000)
                {
                    finished = true;
                    break;
                }
            }

            log.LogWarning($"Purge of {totalDeleted} instance(s) completed after {sw.Elapsed}. Finished = {finished}.");

            return new OkObjectResult(new
            {
                totalDeleted,
                finished,
            });
        }
    }
}
