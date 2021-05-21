// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    static class Common
    {
        public static async Task<string> ScheduleManyInstances(
            IDurableOrchestrationClient client,
            ILogger log,
            string orchestrationName,
            int count,
            string prefix)
        {
            DateTime utcNow = DateTime.UtcNow;
            prefix += utcNow.ToString("yyyyMMdd-hhmmss");

            log.LogWarning($"Scheduling {count} orchestration(s) with a prefix of '{prefix}'...");

            await Enumerable.Range(0, count).ParallelForEachAsync(200, i =>
            {
                string instanceId = $"{prefix}-{i:X16}";
                return client.StartNewAsync(orchestrationName, instanceId);
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

        public static async Task ParallelForEachAsync<T>(this IEnumerable<T> items, int maxConcurrency, Func<T, Task> action)
        {
            List<Task> tasks;
            if (items is ICollection<T> itemCollection)
            {
                tasks = new List<Task>(itemCollection.Count);
            }
            else
            {
                tasks = new List<Task>();
            }

            using var semaphore = new SemaphoreSlim(maxConcurrency);
            foreach (T item in items)
            {
                tasks.Add(InvokeThrottledAction(item, action, semaphore));
            }

            await Task.WhenAll(tasks);
        }

        static async Task InvokeThrottledAction<T>(T item, Func<T, Task> action, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                await action(item);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
