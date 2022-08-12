// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

[assembly: Microsoft.Azure.WebJobs.Hosting.WebJobsStartup(
    typeof(DurableTask.SqlServer.AzureFunctions.SqlDurabilityProviderStartup))]

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Hosting;
    using Microsoft.Extensions.DependencyInjection;


    class SqlDurabilityProviderStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            builder.Services.AddSingleton<IDurabilityProviderFactory, SqlDurabilityProviderFactory>();
        }
    }
}
