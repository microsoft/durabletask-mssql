// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    static class Utils
    {
        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForCompletionAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            TimeSpan timeout)
        {
            using CancellationTokenSource cts = new CancellationTokenSource(timeout);
            return await client.WaitForCompletionAsync(instanceId, cts.Token);
        }

        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForCompletionAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                DurableOrchestrationStatus status = await client.GetStatusAsync(instanceId);
                switch (status?.RuntimeStatus)
                {
                    case OrchestrationRuntimeStatus.Canceled:
                    case OrchestrationRuntimeStatus.Completed:
                    case OrchestrationRuntimeStatus.Failed:
                    case OrchestrationRuntimeStatus.Terminated:
                        return status;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();

            // Code should never reach here
            return null!;
        }

        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForStartAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            TimeSpan timeout)
        {
            using CancellationTokenSource cts = new CancellationTokenSource(timeout);
            return await client.WaitForStartAsync(instanceId, cts.Token);
        }

        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForStartAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                DurableOrchestrationStatus status = await client.GetStatusAsync(instanceId);
                if (status != null && status.RuntimeStatus != OrchestrationRuntimeStatus.Pending)
                {
                    return status;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }

            cancellationToken.ThrowIfCancellationRequested();

            // Code should never reach here
            return null!;
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
