// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable
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
    using SemVersion;
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
                "schema-1.0.0.sql",
                "schema-1.2.0.sql",
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
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            await this.ValidateDatabaseSchemaAsync(testDb);

            // Create the DB schema again - should be a no-op since it already exists
            this.logProvider.Clear();
            await service.CreateIfNotExistsAsync();
            await this.ValidateDatabaseSchemaAsync(testDb);

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
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanCreateAndDropSchemaWithCustomSchemaName(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            string schemaName = "testSchemaName";
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb,schemaName);

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
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted($"{schemaName}._UpdateVersion"))
                .EndOfLog();

            await this.ValidateDatabaseSchemaAsync(testDb, schemaName);
            
            this.logProvider.Clear();
            await service.CreateIfNotExistsAsync();
            await this.ValidateDatabaseSchemaAsync(testDb, schemaName);
            
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.CheckedDatabase(),
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted($"{schemaName}._GetVersions"));
            
            this.logProvider.Clear();
            await service.DeleteAsync();
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.ExecutedSqlScript("drop-schema.sql"));
            
            Assert.DoesNotContain($"{schemaName}", testDb.GetSchemas());
            Assert.DoesNotContain("dt", testDb.GetSchemas());

        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanCreateAndDropMultipleSchemas(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            const string firstTestSchemaName = "firstTestSchemaName";
            const string secondTestSchemaName = "secondTestSchemaName";
            IOrchestrationService firstService = this.CreateServiceWithTestDb(testDb, firstTestSchemaName);
            IOrchestrationService secondService = this.CreateServiceWithTestDb(testDb, secondTestSchemaName);
            
            await firstService.CreateAsync(recreateInstanceStore: true);

            await this.ValidateDatabaseSchemaAsync(testDb, firstTestSchemaName);

            await secondService.CreateAsync(recreateInstanceStore: true);
            
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
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted($"{firstTestSchemaName}._UpdateVersion"))
                .Expect(
                    LogAssert.CheckedDatabase())
                .Expect(
                    LogAssert.AcquiredAppLock(),
                    LogAssert.ExecutedSqlScript("drop-schema.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted($"{secondTestSchemaName}._UpdateVersion"))
                .EndOfLog();

            await this.ValidateDatabaseSchemaAsync(testDb, secondTestSchemaName);
            
            this.logProvider.Clear();
            await firstService.CreateIfNotExistsAsync();
            await this.ValidateDatabaseSchemaAsync(testDb, firstTestSchemaName);
            
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.CheckedDatabase(),
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted($"{firstTestSchemaName}._GetVersions"));

            this.logProvider.Clear();
            await secondService.CreateIfNotExistsAsync();
            await this.ValidateDatabaseSchemaAsync(testDb, secondTestSchemaName);

            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.CheckedDatabase(),
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted($"{secondTestSchemaName}._GetVersions"));

            Assert.Contains($"{firstTestSchemaName}", testDb.GetSchemas());
            Assert.Contains($"{secondTestSchemaName}", testDb.GetSchemas());
            
            this.logProvider.Clear();
            await firstService.DeleteAsync();
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.ExecutedSqlScript("drop-schema.sql"));
            
            Assert.DoesNotContain($"{firstTestSchemaName}",testDb.GetSchemas());
            Assert.Contains($"{secondTestSchemaName}", testDb.GetSchemas());
            
            this.logProvider.Clear();
            await secondService.DeleteAsync();
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.ExecutedSqlScript("drop-schema.sql"));
            
            Assert.DoesNotContain($"{secondTestSchemaName}",testDb.GetSchemas());
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
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
                    LogAssert.ExecutedSqlScript("logic.sql"),
                    LogAssert.ExecutedSqlScript("permissions.sql"),
                    LogAssert.SprocCompleted("dt._UpdateVersion"))
                .EndOfLog();

            await this.ValidateDatabaseSchemaAsync(testDb);
        }

        /// <summary>
        /// Verifies that CreateIfNotExistsAsync is thread-safe.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task SchemaCreationIsSerializedAndIdempotent(bool isDatabaseMissing)
        {
            using TestDatabase testDb = this.CreateTestDb(!isDatabaseMissing);
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            // Simulate 4 workers starting up concurrently and trying to initialize
            // the same database schema. It should just work with predictable output.
            await Enumerable.Range(0, 4).ParallelForEachAsync(4, i => service.CreateIfNotExistsAsync());

            await this.ValidateDatabaseSchemaAsync(testDb);

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
                    LogAssert.ExecutedSqlScript("schema-1.0.0.sql"),
                    LogAssert.ExecutedSqlScript("schema-1.2.0.sql"),
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

        IOrchestrationService CreateServiceWithTestDb(TestDatabase testDb, string? schemaName = null)
        {
            var options = new SqlOrchestrationServiceSettings(testDb.ConnectionString, null, schemaName)
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

        async Task ValidateDatabaseSchemaAsync(TestDatabase database, string schemaName = "dt")
        {
            var expectedTableNames = new HashSet<string>(StringComparer.Ordinal)
            {
                $"{schemaName}.NewEvents",
                $"{schemaName}.NewTasks",
                $"{schemaName}.GlobalSettings",
                $"{schemaName}.History",
                $"{schemaName}.Instances",
                $"{schemaName}.Payloads",
                $"{schemaName}.Versions",
            };

            var expectedSprocNames = new HashSet<string>(StringComparer.Ordinal)
            {
                $"{schemaName}.CreateInstance",
                $"{schemaName}.GetInstanceHistory",
                $"{schemaName}.QuerySingleOrchestration",
                $"{schemaName}.RaiseEvent",
                $"{schemaName}.SetGlobalSetting",
                $"{schemaName}.TerminateInstance",
                $"{schemaName}.PurgeInstanceStateByID",
                $"{schemaName}.PurgeInstanceStateByTime",
                $"{schemaName}._AddOrchestrationEvents",
                $"{schemaName}._CheckpointOrchestration",
                $"{schemaName}._CompleteTasks",
                $"{schemaName}._DiscardEventsAndUnlockInstance",
                $"{schemaName}._GetVersions",
                $"{schemaName}._LockNextOrchestration",
                $"{schemaName}._LockNextTask",
                $"{schemaName}._QueryManyOrchestrations",
                $"{schemaName}._RenewOrchestrationLocks",
                $"{schemaName}._RenewTaskLocks",
                $"{schemaName}._UpdateVersion",
                $"{schemaName}._RewindInstance",
                $"{schemaName}._RewindInstanceRecursive",
            };

            var expectedViewNames = new HashSet<string>(StringComparer.Ordinal)
            {
                $"{schemaName}.vHistory",
                $"{schemaName}.vInstances",
            };

            var expectedFunctionNames = new HashSet<string>(StringComparer.Ordinal)
            {
                $"{schemaName}.CurrentTaskHub",
                $"{schemaName}.GetScaleMetric",
                $"{schemaName}.GetScaleRecommendation",
            };

            // Ensure the schema exists
            Assert.Contains(schemaName, database.GetSchemas());

            // Make sure we've accounted for all expected tables
            foreach (string tableName in database.GetTables(schemaName))
            {
                Assert.Contains(tableName, expectedTableNames);
                expectedTableNames.Remove(tableName);
            }

            Assert.Empty(expectedTableNames);

            // Make sure we've accounted for all expected sprocs
            foreach (string sprocName in database.GetSprocs(schemaName))
            {
                Assert.Contains(sprocName, expectedSprocNames);
                expectedSprocNames.Remove(sprocName);
            }

            Assert.Empty(expectedSprocNames);

            // Make sure we've accounted for all expected views
            foreach (string viewName in database.GetViews(schemaName))
            {
                Assert.Contains(viewName, expectedViewNames);
                expectedViewNames.Remove(viewName);
            }

            Assert.Empty(expectedViewNames);

            // Make sure we've accounted for all expected functions
            foreach (string functionName in database.GetFunctions(schemaName))
            {
                Assert.Contains(functionName, expectedFunctionNames);
                expectedFunctionNames.Remove(functionName);
            }

            Assert.Empty(expectedFunctionNames);

            // Verify that the schema version in the database matches the expected version
            // Note that we'll need to change the expected version here whenever we introduce new schema.
            SemanticVersion currentSchemaVersion = await SharedTestHelpers.GetCurrentSchemaVersionAsync(
                this.output,
                database.ConnectionString,
                schemaName);
            Assert.Equal(1, currentSchemaVersion.Major);
            Assert.Equal(2, currentSchemaVersion.Minor);
            Assert.Equal(1, currentSchemaVersion.Patch);
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

            public IEnumerable<string> GetTables(string schemaName = "dt")
            {
                this.testDb.Tables.Refresh();
                foreach (Table table in this.testDb.Tables)
                {
                    // e.g. "dt.History"
                    if (table.Schema == schemaName)
                    {
                        yield return $"{table.Schema}.{table.Name}";
                    }
                }
            }

            public IEnumerable<string> GetSprocs(string schemaName = "dt")
            {
                this.testDb.StoredProcedures.Refresh();
                foreach (StoredProcedure sproc in this.testDb.StoredProcedures)
                {
                    if (sproc.Schema == "sys")
                    {
                        continue;
                    }

                    // e.g. "dt.LockNextOrchestration"
                    if (sproc.Schema == schemaName)
                    {
                        yield return $"{sproc.Schema}.{sproc.Name}";
                    }
                }
            }

            public IEnumerable<string> GetViews(string schemaName = "dt")
            {
                this.testDb.Views.Refresh();
                foreach (View view in this.testDb.Views)
                {
                    if (view.Schema == "sys" || view.Schema == "INFORMATION_SCHEMA")
                    {
                        continue;
                    }
                    
                    if (view.Schema == schemaName)
                    {
                        yield return $"{view.Schema}.{view.Name}";
                    }
                }
            }

            public IEnumerable<string> GetFunctions(string schemaName = "dt")
            {
                this.testDb.UserDefinedFunctions.Refresh();
                foreach (UserDefinedFunction function in this.testDb.UserDefinedFunctions)
                {
                    if (function.Schema == "sys" || function.Schema == "INFORMATION_SCHEMA")
                    {
                        continue;
                    }

                    if (function.Schema == schemaName)
                    {
                        yield return $"{function.Schema}.{function.Name}";
                    }
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
