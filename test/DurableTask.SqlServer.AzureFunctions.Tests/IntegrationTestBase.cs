﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.ContextImplementations;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.Options;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Xunit;
    using Xunit.Abstractions;

    public class IntegrationTestBase : IAsyncLifetime
    {
        readonly bool multiTenancy;
        readonly string schema;
        readonly string taskHubName;
        readonly TestLogProvider logProvider;
        readonly TestFunctionTypeLocator typeLocator;
        readonly TestSettingsResolver settingsResolver;
        readonly string testName;
        readonly IHost functionsHost;
        readonly IDurableClientFactory clientFactory;
        
        TestCredential? testCredential;

        public IntegrationTestBase(ITestOutputHelper output, string? taskHubName = null, bool multiTenancy = true)
        {
            this.multiTenancy = multiTenancy;
            this.schema = multiTenancy ? "dt" : "dt2";
            this.logProvider = new TestLogProvider(output);
            this.typeLocator = new TestFunctionTypeLocator();
            this.settingsResolver = new TestSettingsResolver();

            this.testName = SharedTestHelpers.GetTestName(output);
            this.taskHubName = taskHubName ?? SharedTestHelpers.GetTestName(output);

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton<INameResolver>(this.settingsResolver);
            serviceCollection.AddSingleton<IConnectionInfoResolver>(this.settingsResolver);
            serviceCollection.AddOptions();
            serviceCollection.Configure<DurableTaskOptions>(options =>
            {
                options.StorageProvider["type"] = "mssql";
                options.StorageProvider["schemaName"] = this.schema;
            });
            serviceCollection.AddDurableTaskSqlProvider();
            serviceCollection.AddDurableClientFactory(a =>
            {
                a.ConnectionName = "SQLDB_Connection";
            });

            this.functionsHost = new HostBuilder()
                .ConfigureLogging(
                    loggingBuilder =>
                    {
                        loggingBuilder.AddProvider(this.logProvider);
                        loggingBuilder.SetMinimumLevel(LogLevel.Information);
                    })
                .ConfigureWebJobs(
                    webJobsBuilder =>
                    {
                        webJobsBuilder.AddDurableTask(options =>
                        {
                            options.HubName = this.taskHubName;
                            options.StorageProvider["type"] = "mssql";
                            options.StorageProvider["schemaName"] = this.schema;
                        });
                    })
                .ConfigureServices(
                    services =>
                    {
                        services.AddSingleton<INameResolver>(this.settingsResolver);
                        services.AddSingleton<IConnectionInfoResolver>(this.settingsResolver);
                        services.AddSingleton<ITypeLocator>(this.typeLocator);
                        services.AddDurableTaskSqlProvider();
                    })
                .Build();
            this.clientFactory = serviceCollection.BuildServiceProvider().GetRequiredService<IDurableClientFactory>();
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
            await SharedTestHelpers.InitializeDatabaseAsync(this.schema);

            // Create a user login specifically for this test to isolate it from other tests
            await SharedTestHelpers.EnableMultiTenancyAsync(this.multiTenancy, this.schema);
            this.testCredential = await SharedTestHelpers.CreateTaskHubLoginAsync(this.testName, this.schema);

            this.settingsResolver.AddSetting("SQLDB_Connection", this.testCredential.ConnectionString);
            await this.functionsHost.StartAsync();
        }

        async Task IAsyncLifetime.DisposeAsync()
        {
            await this.functionsHost.StopAsync();

            // Remove the temporarily-created credentials from the database
            if (this.testCredential != null)
            {
                await SharedTestHelpers.DropTaskHubLoginAsync(this.testCredential, this.schema);
            }
        }

        protected void AddFunctions(Type functionType) => this.typeLocator.AddFunctionType(functionType);

        protected Task CallFunctionAsync(string functionName, string parameterName, object? argument)
        {
            return this.CallFunctionAsync(
                functionName,
                new Dictionary<string, object?>()
                {
                    { parameterName, argument },
                });
        }

        protected Task CallFunctionAsync(string name, IDictionary<string, object?>? args = null)
        {
            IJobHost? jobHost = this.functionsHost.Services.GetService<IJobHost>();
            if (jobHost == null)
            {
                throw new InvalidOperationException("Failed to find the IJobHost service in the Functions host!");
            }

            return jobHost.CallAsync(name, args);
        }

        protected async Task<DurableOrchestrationStatus> RunOrchestrationAsync(
            string name,
            object? input = null,
            string? instanceId = null)
        {
            IDurableClient client = this.GetDurableClient();
            instanceId = await client.StartNewAsync(name, instanceId ?? Guid.NewGuid().ToString("N"), input);

            TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);
            DurableOrchestrationStatus status = await client.WaitForCompletionAsync(instanceId, timeout);
            Assert.NotNull(status);
            return status;
        }

        protected async Task<DurableOrchestrationStatus> StartOrchestrationAsync(
            string name,
            object? input = null,
            string? instanceId = null,
            string? taskHub = null)
        {
            IDurableClient client = this.GetDurableClient(taskHub ?? this.taskHubName);
            instanceId = await client.StartNewAsync(name, instanceId ?? Guid.NewGuid().ToString("N"), input);

            TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);
            DurableOrchestrationStatus status = await client.WaitForStartAsync(instanceId, timeout);
            Assert.NotNull(status);
            return status;
        }


        protected async Task StartOrchestrationWithoutWaitingAsync(
            string name,
            object? input = null,
            string? instanceId = null,
            string? taskHub = null)
        {
            IDurableClient client = this.GetDurableClient(taskHub ?? this.taskHubName);
            instanceId = await client.StartNewAsync(name, instanceId ?? Guid.NewGuid().ToString("N"), input);

            
            Assert.NotNull(instanceId);
        }
        protected async Task<DurableOrchestrationStatus> RewindOrchestrationAsync(string instanceId)
        {
            IDurableClient client = this.GetDurableClient();
#pragma warning disable CS0618 // Type or member is obsolete (preview feature)
            await client.RewindAsync(instanceId, "rewind");
#pragma warning restore CS0618

            TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);
            DurableOrchestrationStatus status = await client.WaitForCompletionAsync(instanceId, timeout);
            Assert.NotNull(status);
            return status;
        }

        protected IDurableClient GetDurableClient(string? taskHubName = null)
        {
            var client = this.clientFactory.CreateClient(new DurableClientOptions
            {
                TaskHub = taskHubName ?? this.taskHubName
            });
            Assert.NotNull(client);
            return client;
        }

        protected IEnumerable<string> GetExtensionLogs()
        {
            return this.GetLogs("Host.Triggers.DurableTask");
        }

        protected IEnumerable<string> GetFunctionLogs(string functionName)
        {
            return this.GetLogs($"Function.{functionName}.User");
        }

        protected IEnumerable<string> GetProviderLogs()
        {
            return this.GetLogs($"DurableTask.SqlServer");
        }

        protected IEnumerable<string> GetLogs(string category)
        {
            bool loggerExists = this.logProvider.TryGetLogs(category, out IReadOnlyCollection<LogEntry> logs);
            Assert.True(loggerExists, $"No logger was found for '{category}'.");

            return logs.Select(entry => entry.Message).ToArray();
        }

        class TestFunctionTypeLocator : ITypeLocator
        {
            readonly List<Type> functionTypes = new List<Type>();

            public void AddFunctionType(Type functionType) => this.functionTypes.Add(functionType);

            IReadOnlyList<Type> ITypeLocator.GetTypes() => this.functionTypes.AsReadOnly();
        }

        class TestSettingsResolver : INameResolver, IConnectionInfoResolver
        {
            readonly Dictionary<string, string> testSettings;
            IConfigurationRoot? config;

            public TestSettingsResolver()
            {
                this.testSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            public void AddSetting(string name, string value)
            {
                if (this.config != null)
                {
                    throw new InvalidOperationException("The configuration has already been loaded!");
                }

                this.testSettings.Add(name, value);
            }

            IConfigurationSection IConnectionInfoResolver.Resolve(string name)
            {
                this.config ??= new ConfigurationBuilder()
                    .AddInMemoryCollection(this.testSettings)
                    .AddEnvironmentVariables()
                    .Build();
                return this.config.GetSection(name);
            }

            string? INameResolver.Resolve(string name)
            {
                if (string.IsNullOrEmpty(name))
                {
                    return null;
                }

                if (this.testSettings.TryGetValue(name, out string? value))
                {
                    return value;
                }

                return Environment.GetEnvironmentVariable(name);
            }
        }
    }
}
