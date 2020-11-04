namespace PerformanceTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    static class Common
    {
        public static string ScheduleManyInstances(
            IDurableOrchestrationClient client,
            string orchestrationName,
            int count,
            ILogger log)
        {
            log.LogWarning($"Scheduling {count} orchestration(s)...");
            DateTime utcNow = DateTime.UtcNow;
            string prefix = utcNow.ToString("yyyyMMdd-hhmmss");
            Parallel.For(0, count, i =>
            {
                // Make unique instance IDs that are semi-ordered
                string instanceId = prefix + "-" + i.ToString("X16");
                client.StartNewAsync(orchestrationName, instanceId).GetAwaiter().GetResult();
            });

            log.LogWarning($"All {count} orchestrations were scheduled successfully!");
            return prefix;
        }

        [FunctionName(nameof(SayHello))]
        public static string SayHello([ActivityTrigger] string name) => $"Hello {name}!";

        public static bool TryGetPositiveIntQueryStringParam(this HttpRequest req, string name, out int value)
        {
            return int.TryParse(req.Query[name], out value) && value > 0;
        }
    }
}
