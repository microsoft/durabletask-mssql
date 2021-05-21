// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;

    public static class ManySequences
    {
        [FunctionName(nameof(StartManySequences))]
        public static async Task<IActionResult> StartManySequences(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (!int.TryParse(req.Query["count"], out int count) || count < 1)
            {
                return new BadRequestObjectResult("A 'count' query string parameter is required and it must contain a positive number.");
            }

            string initialPrefix = (string)req.Query["prefix"] ?? string.Empty;

            string finalPrefix = await Common.ScheduleManyInstances(starter, log, nameof(HelloSequence), count, initialPrefix);
            return new OkObjectResult($"Scheduled {count} orchestrations prefixed with '{finalPrefix}'.");
        }

        [FunctionName(nameof(HelloSequence))]
        public static async Task<List<string>> HelloSequence(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(Common.SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(Common.SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(Common.SayHello), "London"),
                await context.CallActivityAsync<string>(nameof(Common.SayHello), "Amsterdam"),
                await context.CallActivityAsync<string>(nameof(Common.SayHello), "Mumbai")
            };

            return outputs;
        }
    }
}