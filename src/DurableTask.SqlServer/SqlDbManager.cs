// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SemVersion;

    class SqlDbManager
    {
        readonly SqlOrchestrationServiceSettings settings;
        readonly LogHelper traceHelper;

        public SqlDbManager(SqlOrchestrationServiceSettings settings, LogHelper traceHelper)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            this.traceHelper = traceHelper ?? throw new ArgumentNullException(nameof(traceHelper));
        }

        public async Task CreateOrUpgradeSchemaAsync(bool recreateIfExists)
        {
            // Prevent other create or delete operations from executing at the same time.
            await using DatabaseLock dbLock = await this.AcquireDatabaseLockAsync(this.settings.CreateDatabaseIfNotExists);

            var currentSchemaVersion = new SemanticVersion(0, 0, 0);
            if (recreateIfExists)
            {
                await this.DropSchemaAsync(dbLock);
            }
            else
            {
                // If the database already has the latest schema, then skip
                using SqlCommand command = dbLock.CreateCommand();
                command.CommandText = $"{this.settings.SchemaName}._GetVersions";
                command.CommandType = CommandType.StoredProcedure;

                try
                {
                    using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(command, this.traceHelper);
                    if (await reader.ReadAsync())
                    {
                        // The first result contains the latest version
                        currentSchemaVersion = SqlUtils.GetSemanticVersion(reader);
                        if (currentSchemaVersion >= DTUtils.ExtensionVersion)
                        {
                            // The schema is already up-to-date.
                            return;
                        }
                    }
                }
                catch (SqlException e) when (e.Number == 2812 /* Could not find stored procedure */)
                {
                    // Ignore - this is expected for new databases
                }
            }

            // SQL schema setup scripts are embedded resources in the assembly, making them immutable post-build.
            Assembly assembly = typeof(SqlOrchestrationService).Assembly;
            IEnumerable<string> createSchemaFiles = assembly.GetManifestResourceNames()
                .Where(name => name.Contains(".schema-") && name.EndsWith(".sql"));

            var versionedFiles = new Dictionary<SemanticVersion, string>();
            foreach (string name in createSchemaFiles)
            {
                // Attempt to parse the semver-like string from the resource name.
                // This version number tells us whether to execute the script for this extension version.
                const string RegexExpression = @"schema-(\d+.\d+.\d+(?:-\w+)?).sql$";
                Match match = Regex.Match(name, RegexExpression);
                if (!match.Success || match.Groups.Count < 2)
                {
                    throw new InvalidOperationException($"Failed to find version information in resource name '{name}'. The resource name must match the regex expression '{RegexExpression}'.");
                }

                SemanticVersion version = SemanticVersion.Parse(match.Groups[1].Value);
                if (!versionedFiles.TryAdd(version, match.Value))
                {
                    throw new InvalidOperationException($"There must not be more than one script resource with the same version number! Found {version} multiple times.");
                }
            }

            // Sort by the version numbers to ensure that we run them in the correct order
            foreach ((SemanticVersion version, string name) in versionedFiles.OrderBy(pair => pair.Key))
            {
                // Skip past versions that are already present in the database
                if (version > currentSchemaVersion)
                {
                    await this.ExecuteSqlScriptAsync(name, dbLock);
                    currentSchemaVersion = version;
                }
            }

            // Add or update stored procedures, functions, and views
            await this.ExecuteSqlScriptAsync("logic.sql", dbLock);

            // Configure security roles, permissions, etc.
            await this.ExecuteSqlScriptAsync("permissions.sql", dbLock);

            // Insert the current extension version number into the database and commit the transaction.
            // The extension version is used instead of the schema version to more accurately track whether
            // we need to update the sprocs or views.
            using (SqlCommand command = dbLock.CreateCommand())
            {
                command.CommandText = $"{this.settings.SchemaName}._UpdateVersion";
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add("@SemanticVersion", SqlDbType.NVarChar, 100).Value = DTUtils.ExtensionVersion.ToString();

                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
            }

            await dbLock.CommitAsync();
        }

        public async Task DeleteSchemaAsync()
        {
            // Prevent other create or delete operations from executing at the same time.
            await using DatabaseLock dbLock = await this.AcquireDatabaseLockAsync();
            await this.DropSchemaAsync(dbLock);
            await dbLock.CommitAsync();
        }

        Task DropSchemaAsync(DatabaseLock dbLock) => this.ExecuteSqlScriptAsync("drop-schema.sql", dbLock);

        async Task<DatabaseLock> AcquireDatabaseLockAsync(bool createDatabaseIfNotExists = false)
        {
            if (createDatabaseIfNotExists)
            {
                await this.EnsureDatabaseExistsAsync();
            }

            SqlConnection connection = this.settings.CreateConnection();
            await connection.OpenAsync();

            // It's possible that more than one worker may attempt to execute this creation logic at the same
            // time. To avoid update conflicts, we use an app lock + a transaction to ensure only a single worker
            // can perform an upgrade at a time. All other workers will wait for the first one to complete.
            const string LockName = "DURABLE_TASK_SCHEMA_UPGRADE_LOCK";

            SqlTransaction lockTransaction = (SqlTransaction)await connection.BeginTransactionAsync();

            using (SqlCommand command = connection.CreateCommand())
            {
                // Reference https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql#syntax
                command.CommandText = "sys.sp_getapplock";
                command.CommandType = CommandType.StoredProcedure;
                command.Transaction = lockTransaction;
                command.Parameters.Add("@Resource", SqlDbType.NVarChar, 256).Value = LockName;
                command.Parameters.Add("@LockMode", SqlDbType.VarChar, 32).Value = "Update";
                command.Parameters.Add("@LockOwner", SqlDbType.VarChar, 32).Value = "Transaction";
                command.Parameters.Add("@RETURN_VALUE", SqlDbType.SmallInt).Direction = ParameterDirection.ReturnValue;

                // This command will "block" if the app lock is held by another process or thread.
                Stopwatch latencyStopwatch = Stopwatch.StartNew();
                await command.ExecuteNonQueryAsync();
                latencyStopwatch.Stop();

                int returnCode = (int)command.Parameters["@RETURN_VALUE"].Value;
                if (returnCode < 0)
                {
                    throw new InvalidOperationException($"Failed to acquire a lock on resource '{LockName}'. Return code: {returnCode}.");
                }

                this.traceHelper.AcquiredAppLock(returnCode, latencyStopwatch);
            }

            return new DatabaseLock(connection, lockTransaction);
        }

        async Task EnsureDatabaseExistsAsync()
        {
            // Note that we may not be able to connect to the DB, let alone obtain the lock,
            // if the database does not exist yet. So we obtain a connection to the 'master' database for now.
            // If the current connection doesn't have permission to access the master database, this will fail
            // with a "Login failed for user ..." error, in which case we log and return.
            using SqlConnection connection = this.settings.CreateConnection("master");
            try
            {
                await connection.OpenAsync();
            }
            catch (SqlException e) when (e.Number == 18456 /* LOGON_FAILED */)
            {
                this.traceHelper.GenericInfoEvent(
                    "Failed to connect to the master database. The user may not have permission to access the " + 
                    "master database. Skipping database exists check.",
                    instanceId: null);
                return;
            }

            if (!await this.DoesDatabaseExistAsync(this.settings.DatabaseName, connection))
            {
                await this.CreateDatabaseAsync(this.settings.DatabaseName, connection);
            }
        }

        async Task<bool> DoesDatabaseExistAsync(string databaseName, SqlConnection connection)
        {
            using SqlCommand command = connection.CreateCommand();
            command.CommandText = "SELECT 1 FROM sys.databases WHERE name = @databaseName";
            command.Parameters.AddWithValue("@databaseName", databaseName);

            bool exists = (int?)await SqlUtils.ExecuteScalarAsync(command, this.traceHelper) == 1;
            return exists;
        }

        async Task<bool> CreateDatabaseAsync(string databaseName, SqlConnection connection)
        {
            using SqlCommand command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE {SqlIdentifier.Escape(databaseName)} COLLATE Latin1_General_100_BIN2_UTF8";

            try
            {
                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper);
                this.traceHelper.CreatedDatabase(databaseName);
                return true;
            }
            catch (SqlException e) when (e.Number == 1801 /* Database already exists */)
            {
                // Ignore
                return false;
            }
        }

        async Task ExecuteSqlScriptAsync(string scriptName, DatabaseLock dbLock)
        {
            if (dbLock == null)
            {
                throw new ArgumentNullException(nameof(dbLock));
            }

            if (!dbLock.IsHeld)
            {
                throw new ArgumentException("This database lock has already been released!", nameof(dbLock));
            }

            string scriptText = await GetScriptTextAsync(scriptName);
            
            scriptText = scriptText.Replace("__SchemaNamePlaceholder__", this.settings.SchemaName);
            
            // Split script into distinct executeable commands
            IEnumerable<string> scriptCommands = Regex.Split(scriptText, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)
                .Where(x => x.Trim().Length > 0);

            Stopwatch latencyStopwatch = Stopwatch.StartNew();
            try
            {
                foreach (string commandText in scriptCommands)
                {
                    using SqlCommand command = dbLock.CreateCommand();
                    command.CommandText = commandText;

                    await command.ExecuteNonQueryAsync();
                }
            }
            finally
            {
                latencyStopwatch.Stop();
                this.traceHelper.ExecutedSqlScript(scriptName, latencyStopwatch);
            }
        }

        static Task<string> GetScriptTextAsync(string scriptName, Assembly? assembly = null)
        {
            if (assembly == null)
            {
                assembly = typeof(SqlOrchestrationService).Assembly;
            }

            string assemblyName = assembly.GetName().Name;
            if (!scriptName.StartsWith(assemblyName))
            {
                scriptName = $"{assembly.GetName().Name}.Scripts.{scriptName}";
            }

            using Stream resourceStream = assembly.GetManifestResourceStream(scriptName);
            if (resourceStream == null)
            {
                throw new ArgumentException($"Could not find assembly resource named '{scriptName}'.");
            }

            using var reader = new StreamReader(resourceStream);
            return reader.ReadToEndAsync();
        }

        sealed class DatabaseLock : IAsyncDisposable
        {
            readonly SqlConnection connection;
            readonly SqlTransaction transaction;

            bool committed;

            public DatabaseLock(SqlConnection connection, SqlTransaction transaction)
            {
                this.connection = connection;
                this.transaction = transaction;
            }

            public bool IsHeld => !this.committed;

            public SqlCommand CreateCommand()
            {
                SqlCommand command = this.connection.CreateCommand();
                command.Transaction = this.transaction;
                return command;
            }

            public Task CommitAsync()
            {
                this.committed = true;
                return this.transaction.CommitAsync();
            }

            public async ValueTask DisposeAsync()
            {
                if (!this.committed)
                {
                    await this.transaction.RollbackAsync();
                }

                await this.connection.CloseAsync();
            }
        }
    }
}
