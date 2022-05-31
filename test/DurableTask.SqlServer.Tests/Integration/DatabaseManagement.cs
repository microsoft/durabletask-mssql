// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
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

    public class DatabaseManagement
    {
        readonly TestLogProvider logProvider;
        readonly ITestOutputHelper output;

        public DatabaseManagement(ITestOutputHelper output)
        {
            this.logProvider = new TestLogProvider(output);
            this.output = output;
        }

        /// <summary>
        /// BOM check for all expected schema script files. This test should
        /// fail if scripts are added or removed unintentionally.
        /// </summary>
        [Fact]
        public void CanEnumerateEmbeddedSqlScripts()
        {
            // Starting in 1.0.0, elements can be added but must NEVER be removed!
            var expectedScriptFiles = new HashSet<string>
            {
                "drop-schema.sql",
                "schema-0.2.0.sql",
                "logic.sql",
                "permissions.sql",
            };

            // The actual prefix value may change if the project structure changes.
            Assembly productAssembly = typeof(SqlOrchestrationService).Assembly;
            string prefix = $"{productAssembly.GetName().Name}.Scripts.";

            // Consider only the non-prefixed part of the file name (should match the source code name)
            string[] actualScriptFiles = productAssembly
                .GetManifestResourceNames()
                .Where(name => name.EndsWith(".sql"))
                .Select(path => path.Replace(prefix, string.Empty))
                .ToArray();

            Assert.Equal(expectedScriptFiles.Count, actualScriptFiles.Length);
            foreach (string actual in actualScriptFiles)
            {
                Assert.Contains(actual, expectedScriptFiles);
            }
        }

        /// <summary>
        /// Verifies that the schema can be created the first time correctly,
        /// that the CreateItNotExist API doesn't attempt to create it again,
        /// and then that the schema can be subsequently deleted.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanCreateAndDropSchema(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            // Create the DB schema for the first time
            await service.CreateAsync(recreateInstanceStore: true);

            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert
                .For(this.logProvider)
                .Expect(
                    LogAssert.CheckedDatabase())
                .ExpectIf(
                    isDatabaseMissing,
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"),
                    LogAssert.CreatedDatabase(testDb.Name))
                .Expect(
                    LogAssert.AcquiredAppLock(),
                    LogAssert.ExecutedSqlScript("drop-schema.sql"),
                    LogAssert.ExecutedSqlScript("schema-0.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            ValidateDatabaseSchema(testDb);

            // Create the DB schema again - should be a no-op since it already exists
            this.logProvider.Clear();
            await service.CreateIfNotExistsAsync();
            ValidateDatabaseSchema(testDb);

            // The subsequent execution should run exactly one sproc and no scripts.
            // It's important to verify this to ensure the overhead of CreateIfNotExistsAsync is very small.
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.CheckedDatabase(),
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted("dt._GetVersions"));

            // Delete the database and validate
            this.logProvider.Clear();
            await service.DeleteAsync();
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.ExecutedSqlScript("drop-schema.sql"));

            // The previous schema validation ensures all objects are in the "dt" schema.
            // We know that all objects were successfully removed if the "dt" no longer exists.
            Assert.DoesNotContain("dt", testDb.GetSchemas());
        }

        /// <summary>
        /// Verifies that the CreateIfNotExistsAsync API can correctly initialize the
        /// DB schema. The previous test covered CreateAsync from scratch. This one covers
        /// CreateIfNotExistsAsync from scratch.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanCreateIfNotExists(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            await service.CreateIfNotExistsAsync();

            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert
                .For(this.logProvider)
                .Expect(
                    LogAssert.CheckedDatabase())
                .ExpectIf(
                    isDatabaseMissing,
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"),
                    LogAssert.CreatedDatabase(testDb.Name))
                .Expect(
                    LogAssert.AcquiredAppLock(),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    LogAssert.ExecutedSqlScript("schema-0.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            ValidateDatabaseSchema(testDb);
        }

        /// <summary>
        /// Verifies that CreateIfNotExistsAsync is thread-safe.
        /// </summary>
        /// <returns></returns>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void SchemaCreationIsSerializedAndIdempotent(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            // Simulate 4 workers starting up concurrently and trying to initialize
            // the same database schema. It should just work with predictable output.
            Parallel.For(0, 4, i =>
            {
                service.CreateIfNotExistsAsync().GetAwaiter().GetResult();
            });

            ValidateDatabaseSchema(testDb);

            // Operations are expected to be serialized, making the log output deterministic.
            LogAssert
                .For(this.logProvider)
                .Expect(
                    // At least 1 worker will check the database first
                    LogAssert.CheckedDatabase())
                .Contains(
                    // The other 3 workers will check in some non-deterministic order
                    LogAssert.CheckedDatabase(),
                    LogAssert.CheckedDatabase(),
                    LogAssert.CheckedDatabase())
                .ContainsIf(
                    // One worker may obtain the lock after another worker created the database.
                    isDatabaseMissing,
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"),
                    LogAssert.CreatedDatabase(testDb.Name))
                .OptionallyContainsIf(
                    // Anywhere from 0 to 3 of the workers may end up attempting to create the database.
                    isDatabaseMissing,
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"),
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"),
                    LogAssert.CommandCompleted($"CREATE DATABASE [{testDb.Name}]"))
                .Expect(
                    // 1st
                    LogAssert.AcquiredAppLock(statusCode: 0),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    LogAssert.ExecutedSqlScript("schema-0.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"),
                    // 2nd
                    LogAssert.AcquiredAppLock(),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    // 3rd
                    LogAssert.AcquiredAppLock(),
                    LogAssert.SprocCompleted("dt._GetVersions"),
                    // 4th
                    LogAssert.AcquiredAppLock(),
                    LogAssert.SprocCompleted("dt._GetVersions"))
                .EndOfLog();
        }

        TestDatabase CreateTestDb(bool initializeDatabase = true)
        {
            var testDb = new TestDatabase(this.output);
            if (initializeDatabase)
            {
                testDb.Create();
            }

            return testDb;
        }

        IOrchestrationService CreateServiceWithTestDb(TestDatabase testDb)
        {
            var options = new SqlOrchestrationServiceSettings(testDb.ConnectionString)
            {
                CreateDatabaseIfNotExists = true,
                LoggerFactory = LoggerFactory.Create(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Trace);
                    builder.AddProvider(this.logProvider);
                }),
            };

            return new SqlOrchestrationService(options);
        }

        static void ValidateDatabaseSchema(TestDatabase database)
        {
            var expectedTableNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.NewEvents",
                "dt.NewTasks",
                "dt.GlobalSettings",
                "dt.History",
                "dt.Instances",
                "dt.Payloads",
                "dt.Versions",
            };

            var expectedSprocNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.CreateInstance",
                "dt.GetInstanceHistory",
                "dt.QuerySingleOrchestration",
                "dt.RaiseEvent",
                "dt.SetGlobalSetting",
                "dt.TerminateInstance",
                "dt.PurgeInstanceStateByID",
                "dt.PurgeInstanceStateByTime",
                "dt._AddOrchestrationEvents",
                "dt._CheckpointOrchestration",
                "dt._CompleteTasks",
                "dt._GetVersions",
                "dt._LockNextOrchestration",
                "dt._LockNextTask",
                "dt._QueryManyOrchestrations",
                "dt._RenewOrchestrationLocks",
                "dt._RenewTaskLocks",
                "dt._UpdateVersion",
                "dt._RewindInstance",
                "dt._RewindInstanceRecursive",
            };

            var expectedViewNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.vHistory",
                "dt.vInstances",
            };

            var expectedFunctionNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.CurrentTaskHub",
                "dt.GetScaleMetric",
                "dt.GetScaleRecommendation",
            };

            // Ensure the schema exists
            Assert.Contains("dt", database.GetSchemas());

            // Make sure we've accounted for all expected tables
            foreach (string tableName in database.GetTables())
            {
                Assert.Contains(tableName, expectedTableNames);
                expectedTableNames.Remove(tableName);
            }

            Assert.Empty(expectedTableNames);

            // Make sure we've accounted for all expected sprocs
            foreach (string sprocName in database.GetSprocs())
            {
                Assert.Contains(sprocName, expectedSprocNames);
                expectedSprocNames.Remove(sprocName);
            }

            Assert.Empty(expectedSprocNames);

            // Make sure we've accounted for all expected views
            foreach (string viewName in database.GetViews())
            {
                Assert.Contains(viewName, expectedViewNames);
                expectedViewNames.Remove(viewName);
            }

            Assert.Empty(expectedViewNames);

            // Make sure we've accounted for all expected functions
            foreach (string functionName in database.GetFunctions())
            {
                Assert.Contains(functionName, expectedFunctionNames);
                expectedFunctionNames.Remove(functionName);
            }

            Assert.Empty(expectedFunctionNames);
        }

        sealed class TestDatabase : IDisposable
        {
            readonly Server server;
            readonly Database testDb;
            readonly ITestOutputHelper output;
            bool created = false;

            public TestDatabase(ITestOutputHelper output)
            {
                string defaultConnectionString = SharedTestHelpers.GetDefaultConnectionString("master");
                this.server = new Server(new ServerConnection(new SqlConnection(defaultConnectionString)));
                this.testDb = new Database(this.server, $"TestDB_{DateTime.UtcNow:yyyyMMddhhmmssfffffff}")
                {
                    Collation = "Latin1_General_100_BIN2_UTF8"
                };

                this.ConnectionString =
                    new SqlConnectionStringBuilder(this.server.ConnectionContext.ConnectionString)
                    {
                        InitialCatalog = this.testDb.Name,
                    }
                    .ConnectionString;

                this.output = output;
            }

            public string ConnectionString { get; }

            public string Name => this.testDb.Name;

            public void Create()
            {
                this.output.WriteLine($"Creating database: {this.server.Name}/{this.Name}");
                this.testDb.Create();
                this.created = true;
            }

            public IEnumerable<string> GetSchemas()
            {
                this.testDb.Schemas.Refresh();
                foreach (Schema schema in this.testDb.Schemas)
                {
                    yield return schema.Name;
                }
            }

            public IEnumerable<string> GetTables()
            {
                this.testDb.Tables.Refresh();
                foreach (Table table in this.testDb.Tables)
                {
                    // e.g. "dt.History"
                    yield return $"{table.Schema}.{table.Name}";
                }
            }

            public IEnumerable<string> GetSprocs()
            {
                this.testDb.StoredProcedures.Refresh();
                foreach (StoredProcedure sproc in this.testDb.StoredProcedures)
                {
                    if (sproc.Schema == "sys")
                    {
                        continue;
                    }

                    // e.g. "dt.LockNextOrchestration"
                    yield return $"{sproc.Schema}.{sproc.Name}";
                }
            }

            public IEnumerable<string> GetViews()
            {
                this.testDb.Views.Refresh();
                foreach (View view in this.testDb.Views)
                {
                    if (view.Schema == "sys" || view.Schema == "INFORMATION_SCHEMA")
                    {
                        continue;
                    }

                    yield return $"{view.Schema}.{view.Name}";
                }
            }

            public IEnumerable<string> GetFunctions()
            {
                this.testDb.UserDefinedFunctions.Refresh();
                foreach (UserDefinedFunction function in this.testDb.UserDefinedFunctions)
                {
                    if (function.Schema == "sys" || function.Schema == "INFORMATION_SCHEMA")
                    {
                        continue;
                    }

                    yield return $"{function.Schema}.{function.Name}";
                }
            }

            public void Dispose()
            {
                if (this.created)
                {
                    this.output.WriteLine($"Dropping database: {this.server.Name}/{this.Name}");
                    this.server.KillDatabase(this.Name);
                }
            }
        }
    }
}
