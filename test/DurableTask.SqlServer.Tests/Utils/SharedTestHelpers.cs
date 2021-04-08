// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public static class SharedTestHelpers
    {
        public static string GetDefaultConnectionString()
        {
            // The default for local development on a Windows OS
            string defaultConnectionString = "Server=localhost;Database=DurableDB;Trusted_Connection=True;";

            // The use of SA_PASSWORD is intended for use with the mssql docker container
            string saPassword = Environment.GetEnvironmentVariable("SA_PASSWORD");
            if (string.IsNullOrEmpty(saPassword))
            {
                return defaultConnectionString;
            }

            var builder = new SqlConnectionStringBuilder(defaultConnectionString)
            {
                IntegratedSecurity = false,
                UserID = "sa",
                Password = saPassword,
            };

            return builder.ToString();
        }

        public static TimeSpan AdjustForDebugging(this TimeSpan timeout)
        {
            if (timeout == default)
            {
                timeout = TimeSpan.FromSeconds(10);
            }

            if (Debugger.IsAttached)
            {
                TimeSpan debuggingTimeout = TimeSpan.FromMinutes(5);
                if (debuggingTimeout > timeout)
                {
                    timeout = debuggingTimeout;
                }
            }

            return timeout;
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
