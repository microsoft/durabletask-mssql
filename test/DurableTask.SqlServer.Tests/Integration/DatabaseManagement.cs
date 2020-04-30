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
            // Elements can be added but must NEVER be removed!
            var expectedScriptFiles = new HashSet<string>
            {
                "drop-schema.sql",
                "schema-0.1.0.sql",
                "sprocs.sql",
            };

            // The actual prefix value may change if the project structure changes.
            Assembly productAssembly = typeof(SqlServerOrchestrationService).Assembly;
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
        [Fact]
        public async Task CanCreateAndDropSchema()
        {
            using TestDatabase testDb = this.CreateTestDb();
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            // Create the DB schema for the first time
            await service.CreateAsync(recreateInstanceStore: true);

            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.ExecutedSqlScript("drop-schema.sql"),
                LogAssert.ExecutedSqlScript("schema-0.1.0.sql"),
                LogAssert.ExecutedSqlScript("sprocs.sql"),
                LogAssert.SprocCompleted("dt.UpdateVersion"));

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
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted("dt.GetVersions"));

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
        [Fact]
        public async Task CanCreateIfNotExists()
        {
            using TestDatabase testDb = this.CreateTestDb();
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            await service.CreateIfNotExistsAsync();

            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.SprocCompleted("dt.GetVersions"),
                LogAssert.ExecutedSqlScript("schema-0.1.0.sql"),
                LogAssert.ExecutedSqlScript("sprocs.sql"),
                LogAssert.SprocCompleted("dt.UpdateVersion"));

            ValidateDatabaseSchema(testDb);
        }

        /// <summary>
        /// Verifies that CreateIfNotExistsAsync is thread-safe.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public void SchemaCreationIsSerializedAndIdempotent()
        {
            using TestDatabase testDb = this.CreateTestDb();
            IOrchestrationService service = this.CreateServiceWithTestDb(testDb);

            // Simulate 10 workers starting up concurrently and trying to initialize
            // the same database schema. It should just work with predictable output.
            Parallel.For(0, 10, i =>
            {
                service.CreateIfNotExistsAsync().GetAwaiter().GetResult();
            });

            ValidateDatabaseSchema(testDb);

            // Operations are expected to be serialized, making the log output deterministic.
            LogAssert.Sequence(
                this.logProvider,
                // 1st
                LogAssert.AcquiredAppLock(statusCode: 0),
                LogAssert.SprocCompleted("dt.GetVersions"),
                LogAssert.ExecutedSqlScript("schema-0.1.0.sql"),
                LogAssert.ExecutedSqlScript("sprocs.sql"),
                LogAssert.SprocCompleted("dt.UpdateVersion"),
                // 2nd
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 3rd
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 4th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 5th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 6th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 7th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 8th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 9th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"),
                // 10th
                LogAssert.AcquiredAppLock(statusCode: 1),
                LogAssert.SprocCompleted("dt.GetVersions"));
        }

        TestDatabase CreateTestDb()
        {
            var testDb = new TestDatabase(this.output);
            testDb.Create();
            return testDb;
        }

        IOrchestrationService CreateServiceWithTestDb(TestDatabase testDb)
        {
            var options = new SqlServerProviderOptions
            {
                ConnectionString = testDb.ConnectionString,
                LoggerFactory = LoggerFactory.Create(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Trace);
                    builder.AddProvider(this.logProvider);
                }),
            };

            return new SqlServerOrchestrationService(options);
        }

        static void ValidateDatabaseSchema(TestDatabase database)
        {
            var expectedTableNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.NewEvents",
                "dt.NewTasks",
                "dt.History",
                "dt.Instances",
                "dt.Versions",
            };

            var expectedSprocNames = new HashSet<string>(StringComparer.Ordinal)
            {
                "dt.LockNextOrchestration",
                "dt.CheckpointOrchestration",
                "dt.CreateInstances",
                "dt.QuerySingleOrchestration",
                "dt.CompleteTasks",
                "dt.LockNextTask",
                "dt.GetVersions",
                "dt.UpdateVersion",
            };

            var expectedViewNames = new HashSet<string>(StringComparer.Ordinal)
            {
                // TODO: Add view names here
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
        }

        sealed class TestDatabase : IDisposable
        {
            readonly Server server;
            readonly Database testDb;
            readonly ITestOutputHelper output;

            public TestDatabase(ITestOutputHelper output)
            {
                string databaseName = $"TestDB_{DateTime.UtcNow:yyyyMMddhhmmssfffffff}";

                this.server = new Server(new ServerConnection(new SqlServerProviderOptions().CreateConnection()));
                this.testDb = new Database(this.server, databaseName)
                {
                    Collation = "Latin1_General_100_BIN2_UTF8",
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

            public void Create()
            {
                this.output.WriteLine($"Creating database: {this.server.Name}/{this.testDb.Name}");
                this.testDb.Create();
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

            public void Dispose()
            {
                this.output.WriteLine($"Dropping database: {this.server.Name}/{this.testDb.Name}");
                this.server.KillDatabase(this.testDb.Name);
            }
        }
    }
}
