namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
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
        public static IActionResult StartManySequences(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (!int.TryParse(req.Query["count"], out int count) || count < 1)
            {
                return new BadRequestObjectResult("A 'count' query string parameter is required and it must contain a positive number.");
            }

            // Queue on a thread-pool thread to avoid problems with HTTP timeouts
            log.LogWarning($"Scheduling {count} orchestrations using background threads.");
            ThreadPool.QueueUserWorkItem(delegate
            {
                string prefix = DateTime.UtcNow.ToString("yyyyMMdd-hhmmss");
                Parallel.For(0, count, i =>
                {
                    // Make unique instance IDs that are semi-ordered
                    string instanceId = prefix + "-" + i.ToString("X16");
                    starter.StartNewAsync(nameof(HelloSequence), instanceId).GetAwaiter().GetResult();
                });

                log.LogWarning($"All {count} orchestrations were scheduled successfully!");
            });

            return new OkResult();
        }

        [FunctionName(nameof(HelloSequence))]
        public static async Task<List<string>> HelloSequence(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(SayHello), "London"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Amsterdam"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Mumbai")
            };

            return outputs;
        }

        [FunctionName(nameof(SayHello))]
        public static string SayHello([ActivityTrigger] string name)
        {
            return $"Hello {name}!";
        }
    }
}