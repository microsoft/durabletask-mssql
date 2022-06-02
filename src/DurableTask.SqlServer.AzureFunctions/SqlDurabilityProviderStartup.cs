// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(
    typeof(DurableTask.SqlServer.AzureFunctions.SqlDurabilityProviderStartup))]

namespace DurableTask.SqlServer.AzureFunctions
{
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.DependencyInjection;

    class SqlDurabilityProviderStartup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<IDurabilityProviderFactory, SqlDurabilityProviderFactory>();
        }
    }
}
