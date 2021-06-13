// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.DependencyInjection;

    /// <summary>
    /// Extension methods for the Microsoft SQL Durable Task storage provider.
    /// </summary>
    public static class SqlDurabilityProviderExtensions
    {
        /// <summary>
        /// Adds Durable Task SQL storage provider services to the specified <see cref="IServiceCollection"/>.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> for adding services.</param>
        public static void AddDurableTaskSqlProvider(this IServiceCollection services)
        {
            services.AddSingleton<IDurabilityProviderFactory, SqlDurabilityProviderFactory>();
        }
    }
}
