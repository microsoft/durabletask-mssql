// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable
namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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
    using SemVersion;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class UpgradeTests
    {
        static readonly TimeSpan RestoreTimeout = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Orchestrations resume against a freshly restored database with a cold plan cache, hence the larger timeout value.
        /// </summary>
        static readonly TimeSpan OrchestrationTimeout = TimeSpan.FromSeconds(60);

        readonly TestLogProvider logProvider;
        readonly ITestOutputHelper output;

        public UpgradeTests(ITestOutputHelper output)
        {
            this.logProvider = new TestLogProvider(output);
            this.output = output;
        }

        // TODO: Support validation from multiple base versions
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

            // Sanity check: the restored database must be at the version we're upgrading from.
            SemanticVersion expectedVersion = SemanticVersion.Parse(version);
            SemanticVersion restoredVersion = await SharedTestHelpers.GetCurrentSchemaVersionAsync(this.output, dbConnectionString);
            Assert.Equal(expectedVersion.Major, restoredVersion.Major);
            Assert.Equal(expectedVersion.Minor, restoredVersion.Minor);
            Assert.Equal(expectedVersion.Patch, restoredVersion.Patch);

            // This call should update the existing database schema to the latest version
            await service.CreateIfNotExistsAsync();

            // Check the logs to ensure everything we expected to happen actually happened
            LogAssert
                .For(this.logProvider)
                .Expect(
                    LogAssert.AcquiredAppLock(statusCode: 0),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.6.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            await this.VerifyExpectedRuntimeData(service);
            await this.CompletePendingInstances(service, loggerFactory);
        }

        string RestoreDatabaseFromBackup(string version)
        {
            SqlConnectionStringBuilder builder = new(SharedTestHelpers.GetDefaultConnectionString(database: "master"));
            Server dbServer = new(new ServerConnection(new SqlConnection(builder.ToString())));

            // Restoring a multi-megabyte backup can take a while on a cold container.
            dbServer.ConnectionContext.StatementTimeout = (int)RestoreTimeout.TotalSeconds;

            string dbName = $"DurableDB-v{version}";
            if (dbServer.Databases[dbName] != null)
            {
                // Drop any previous database with this name, disconnecting any existing sessions
                this.output.WriteLine($"Dropping existing '{dbName}' database...");
                dbServer.KillDatabase(dbName);
            }

            string backupFileName = Path.Combine("DatabaseBackups", $"DurableDB-v{version}.bak.zip");
            Assert.True(File.Exists(backupFileName), $"Could not find the database backup at '{Path.GetFullPath(backupFileName)}'.");

            string extractedBackupFile = Path.Combine(Path.GetTempPath(), $"DurableDB-v{version}.bak");
            this.output.WriteLine($"Extracting {backupFileName} to {extractedBackupFile}...");
            using (ZipArchive archive = ZipFile.OpenRead(backupFileName))
            {
                archive.Entries[0].ExtractToFile(extractedBackupFile, overwrite: true);
            }

            try
            {
                string serverBackupFile = SqlBackupStaging.Stage(extractedBackupFile, this.output);

                try
                {
                    Restore restore = new()
                    {
                        Database = dbName,
                        ReplaceDatabase = true,
                        Devices = { new BackupDeviceItem(serverBackupFile, DeviceType.File) },
                    };

                    try
                    {
                        // The backup has Windows file paths baked into it, so the data and log files have
                        // to be relocated to wherever this particular server keeps its databases.
                        foreach (RelocateFile relocatedFile in GetRelocatedFiles(dbServer, restore))
                        {
                            restore.RelocateFiles.Add(relocatedFile);
                        }

                        this.output.WriteLine($"Restoring {serverBackupFile} to '{dbName}'...");
                        restore.SqlRestore(dbServer);
                    }
                    catch (Exception e) when (!SqlBackupStaging.IsServerContainerized)
                    {
                        throw new InvalidOperationException(
                            $"Failed to restore '{serverBackupFile}'. The test assumed that SQL Server can read " +
                            $"files from this machine's file system. If SQL Server is running in a container that " +
                            $"the docker CLI can't reach, set the 'DTFX_TEST_SQL_CONTAINER' environment variable " +
                            $"to its name.",
                            e);
                    }
                }
                finally
                {
                    SqlBackupStaging.Unstage(serverBackupFile, this.output);
                }
            }
            finally
            {
                File.Delete(extractedBackupFile);
            }

            builder.InitialCatalog = dbName;
            return builder.ToString();
        }

        static IEnumerable<RelocateFile> GetRelocatedFiles(Server dbServer, Restore restore)
        {
            string dataDirectory = !string.IsNullOrEmpty(dbServer.Settings.DefaultFile) ?
                dbServer.Settings.DefaultFile :
                dbServer.Information.MasterDBPath;

            // The path is interpreted by the server, which may not use the same directory separator as
            // this machine - a Windows test client can be driving a Linux container. Path.Combine would
            // use the client's separator, so the server's own separator is inferred instead.
            char separator = dataDirectory.Contains("/") ? '/' : '\\';
            dataDirectory = dataDirectory.TrimEnd('/', '\\');

            foreach (DataRow row in restore.ReadFileList(dbServer).Rows)
            {
                string logicalName = (string)row["LogicalName"];

                // "L" identifies the log file; everything else is a data file.
                string extension = string.Equals((string)row["Type"], "L", StringComparison.OrdinalIgnoreCase) ? ".ldf" : ".mdf";
                yield return new RelocateFile(logicalName, $"{dataDirectory}{separator}{logicalName}{extension}");
            }
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
            TimeSpan timeout = OrchestrationTimeout.AdjustForDebugging();

            try
            {
                // Test2, which was in Pending, should start and run to completion on its own.
                instance = new OrchestrationInstance { InstanceId = "Test2" };
                state = await client.WaitForOrchestrationAsync(instance, timeout);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal(@"[""Hello, Tokyo!"",""Hello, London!"",""Hello, Seattle!""]", state.Output);

                // Test4, which was Running, is resumed by the WaitForSignal orchestrations
                string data = Guid.NewGuid().ToString();
                instance = new OrchestrationInstance { InstanceId = "Test4" };
                await client.RaiseEventAsync(instance, "signal", data);
                state = await client.WaitForOrchestrationAsync(instance, timeout);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal($"\"{data}\"", state.Output);

                // Test5 is waiting on Test5-child, which is waiting for an external event.
                data = Guid.NewGuid().ToString();
                instance = new OrchestrationInstance { InstanceId = "Test5-child" };
                await client.RaiseEventAsync(instance, "signal", data);
                state = await client.WaitForOrchestrationAsync(instance, timeout);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal($"\"{data}\"", state.Output);
                instance = new OrchestrationInstance { InstanceId = "Test5" };
                state = await client.WaitForOrchestrationAsync(instance, timeout);
                Assert.Equal(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.Equal($"\"{data}\"", state.Output);

                // Test6, which was cut off during an activity execution, should run to completion on its own.
                OrchestrationState test6 = await client.WaitForOrchestrationAsync(
                    new OrchestrationInstance { InstanceId = "Test6" },
                    timeout);
                Assert.Equal(OrchestrationStatus.Completed, test6.OrchestrationStatus);
                Assert.Equal(@"[""Hello, Tokyo!"",""Hello, London!"",""Hello, Seattle!""]", test6.Output);
            }
            finally
            {
                // Stop polling the restored database
                await worker.StopAsync(isForced: true);
            }
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
