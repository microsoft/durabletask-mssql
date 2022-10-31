// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable
namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Microsoft.SqlServer.Management.Common;
    using Microsoft.SqlServer.Management.Smo;
    using Xunit;
    using Xunit.Abstractions;

    public class UpgradeTests
    {
        readonly TestLogProvider logProvider;
        readonly ITestOutputHelper output;

        public UpgradeTests(ITestOutputHelper output)
        {
            this.logProvider = new TestLogProvider(output);
            this.output = output;
        }

        [Theory]
        [InlineData("1.0.0")]
        public async Task ValidateUpgradedOrchestrations(string version)
        {
            string dbConnectionString = this.RestoreDatabaseFromBackup(version);

            ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Trace);
                builder.AddProvider(this.logProvider);
            });

            SqlOrchestrationServiceSettings settings = new(dbConnectionString)
            {
                CreateDatabaseIfNotExists = false,
                LoggerFactory = loggerFactory,
            };

            SqlOrchestrationService service = new(settings);

            // This call should update the existing database schema to the latest version
            await service.CreateIfNotExistsAsync();

            // Check the logs to ensure everything we expected to happen actually happened
            LogAssert
                .For(this.logProvider)
                .Expect(
                    LogAssert.AcquiredAppLock(statusCode: 0),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            // Make sure all the data we expect is there, and that we can query it
            await this.VerifyExpectedRuntimeData(service);

            // Complete all the pending instances, ensuring that they were able to resume successfully
            await this.CompletePendingInstances(service, loggerFactory);
        }

        string RestoreDatabaseFromBackup(string version)
        {
            SqlConnectionStringBuilder builder = new(SharedTestHelpers.GetDefaultConnectionString(database: "master"));
            Server dbServer = new(new ServerConnection(new SqlConnection(builder.ToString())));

            string dbName = $"DurableDB-v{version}";
            Database db = dbServer.Databases[dbName];
            if (db != null)
            {
                this.output.WriteLine($"Dropping existing '{dbName}' database...");

                // Drop any previous databases with this name
                db.UserAccess = DatabaseUserAccess.Restricted;
                db.Alter(TerminationClause.RollbackTransactionsImmediately);
                db.Refresh();
                db.Drop();
            }

            string backupFileName = $"./DatabaseBackups/DurableDB-v{version}.bak.zip";
            Assert.True(File.Exists(backupFileName));

            string extractedBackupFile = Path.Join(Environment.CurrentDirectory, $"DurableDB-v{version}.bak");
            this.output.WriteLine($"Extracting {backupFileName} to {extractedBackupFile}...");
            using (ZipArchive archive = ZipFile.OpenRead(backupFileName))
            {
                archive.Entries[0].ExtractToFile(extractedBackupFile, overwrite: true);
            }

            try
            {
                Restore restore = new()
                {
                    Database = dbName,
                    Devices = { new BackupDeviceItem(extractedBackupFile, DeviceType.File) },
                };

                this.output.WriteLine($"Restoring {extractedBackupFile} to '{dbName}'...");
                restore.SqlRestore(dbServer);
            }
            finally
            {
                File.Delete(extractedBackupFile);
            }

            builder.InitialCatalog = dbName;
            return builder.ToString();
        }

        async Task VerifyExpectedRuntimeData(SqlOrchestrationService service)
        {
            this.output.WriteLine("Verifying contents of the upgraded database...");

            IReadOnlyCollection<OrchestrationState> instances = await service.GetManyOrchestrationsAsync(
                new SqlOrchestrationQuery()
                {
                    FetchInput = true,
                    FetchOutput = true,
                    PageSize = 100,
                },
                CancellationToken.None);

            Assert.Equal(7, instances.Count);

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test1" &&
                i.Status == "Just passing through!" &&
                i.OrchestrationStatus == OrchestrationStatus.Completed &&
                i.Output == @"[""Hello, Tokyo!"",""Hello, London!"",""Hello, Seattle!""]");

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test2" &&
                i.OrchestrationStatus == OrchestrationStatus.Pending);

            Assert.Contains(instances, i =>
                i.OrchestrationInstance.InstanceId == "Test3" &&
                i.Input == @"""signal""" &&
                i.OrchestrationStatus == OrchestrationStatus.Terminated);

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test4" &&
                i.Input == @"""signal""" &&
                i.OrchestrationStatus == OrchestrationStatus.Running);

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test5" &&
                i.Input == @"""signal""" &&
                i.OrchestrationStatus == OrchestrationStatus.Running);

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test5-child" &&
                i.OrchestrationStatus == OrchestrationStatus.Running &&
                i.ParentInstance?.OrchestrationInstance.InstanceId == "Test5");

            Assert.Contains(instances, i => 
                i.OrchestrationInstance.InstanceId == "Test6" &&
                i.Status == "Just passing through!" &&
                i.OrchestrationStatus == OrchestrationStatus.Running);
        }

        async Task CompletePendingInstances(SqlOrchestrationService service, ILoggerFactory loggerFactory)
        {
            this.output.WriteLine("Completing pending instances...");

            // Start the worker, which should allow any in-progress instances to keep running.
            TaskHubWorker worker = new(service, loggerFactory);
            worker.AddTaskOrchestrations(
                new SimpleObjectCreator<TaskOrchestration>(nameof(ActivitySequence), typeof(ActivitySequence)),
                new SimpleObjectCreator<TaskOrchestration>(nameof(WaitForSignal), typeof(WaitForSignal)),
                new SimpleObjectCreator<TaskOrchestration>(nameof(WaitForSignalParent), typeof(WaitForSignalParent)));
            worker.AddTaskActivities(
                new SimpleObjectCreator<TaskActivity>(nameof(SayHello), typeof(SayHello)));
            await worker.StartAsync();

            TaskHubClient client = new(service, loggerFactory: loggerFactory);

            OrchestrationState state;
            OrchestrationInstance instance;
            TimeSpan timeout = TimeSpan.FromSeconds(10);

            // Test2, which was in Pending, should start and run to completion on its own.
            instance = new OrchestrationInstance { InstanceId = "Test2" };
            state = await client.WaitForOrchestrationAsync(instance, timeout);
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal(@"[""Hello, Tokyo!"",""Hello, London!"",""Hello, Seattle!""]", state.Output);

            // Test4, which was Running, is resumed by the WaitForSignal orchestrations
            string data = Guid.NewGuid().ToString();
            instance = new OrchestrationInstance { InstanceId = "Test4" };
            await client.RaiseEventAsync(instance, "signal", data);
            state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal($"\"{data}\"", state.Output);

            // Test5 is waiting on Test5-child, which is waiting for an external event.
            data = Guid.NewGuid().ToString();
            instance = new OrchestrationInstance { InstanceId = "Test5-child" };
            await client.RaiseEventAsync(instance, "signal", data);
            state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal($"\"{data}\"", state.Output);
            instance = new OrchestrationInstance { InstanceId = "Test5" };
            state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
            Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.Equal($"\"{data}\"", state.Output);

            // Test6, which was cut off during an activity execution, should run to completion on its own.
            OrchestrationState test6 = await client.WaitForOrchestrationAsync(
                new OrchestrationInstance { InstanceId = "Test6" },
                TimeSpan.FromSeconds(10));
            Assert.Equal(OrchestrationStatus.Completed, test6.OrchestrationStatus);
            Assert.Equal(@"[""Hello, Tokyo!"",""Hello, London!"",""Hello, Seattle!""]", test6.Output);
        }

        class SimpleObjectCreator<T> : ObjectCreator<T>
        {
            readonly Type type;

            public SimpleObjectCreator(string name, Type type)
            {
                this.Name = name;
                this.type = type;
            }

            public override T Create()
            {
                return (T)Activator.CreateInstance(this.type)!;
            }
        }

        // NOTE: Must be kept consistent with the version defined in tools/TestDBGenerator/Orchestrations.cs
        class ActivitySequence : TaskOrchestration<string[], object>
        {
            public override async Task<string[]> RunTask(OrchestrationContext context, object input)
            {
                string helloTokyo = await context.ScheduleTask<string>(nameof(SayHello), null, "Tokyo");
                string helloLondon = await context.ScheduleTask<string>(nameof(SayHello), null, "London");
                string helloSeattle = await context.ScheduleTask<string>(nameof(SayHello), null, "Seattle");
                return new[] { helloTokyo, helloLondon, helloSeattle };
            }

            public override string GetStatus()
            {
                return "Just passing through!";
            }
        }

        // NOTE: Must be kept consistent with the version defined in tools/TestDBGenerator/Orchestrations.cs.
        //       No need for the logic that blocks activity execution, however.
        class SayHello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return $"Hello, {input}!";
            }
        }

        // NOTE: Must be kept consistent with the version defined in tools/TestDBGenerator/Orchestrations.cs
        class WaitForSignal : TaskOrchestration<string, string>
        {
            readonly TaskCompletionSource<string> tcs = new();
            string? expectedEventName;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                this.expectedEventName = input;
                return await this.tcs.Task;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == this.expectedEventName)
                {
                    this.tcs.SetResult(input);
                }
            }
        }

        // NOTE: Must be kept consistent with the version defined in tools/TestDBGenerator/Orchestrations.cs
        class WaitForSignalParent : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                return await context.CreateSubOrchestrationInstance<string>(
                    name: nameof(WaitForSignal),
                    version: null,
                    instanceId: $"{context.OrchestrationInstance.InstanceId}-child",
                    input: input);
            }
        }
    }
}
