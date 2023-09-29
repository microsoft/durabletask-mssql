// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SemVersion;
    using Xunit.Abstractions;

    public static class SharedTestHelpers
    {
        const string DefaultSchema = "dt";

        public static string GetTestName(ITestOutputHelper output)
        {
            Type type = output.GetType();
            FieldInfo testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            var test = (ITest)testMember.GetValue(output);
            return $"{test.TestCase.TestMethod.Method.Name}" + (test.TestCase.TestMethodArguments == null || test.TestCase.TestMethodArguments.Length == 0 ? string.Empty : $"_{ string.Join("_",test.TestCase.TestMethodArguments.Select(a => a.ToString()))}") ;
        }

        public static string GetDefaultConnectionString(string database = "DurableDB")
        {
            // The default for local development on a Windows OS
            string defaultConnectionString = $"Server=localhost;Database={database};Trusted_Connection=True;Encrypt=False;";
            var builder = new SqlConnectionStringBuilder(defaultConnectionString);

            // The use of SA_PASSWORD is intended for use with the mssql docker container
            string saPassword = Environment.GetEnvironmentVariable("SA_PASSWORD");
            if (!string.IsNullOrEmpty(saPassword))
            {
                builder.IntegratedSecurity = false;
                builder.UserID = "sa";
                builder.Password = saPassword;
            }

            // Overrides for ad-hoc testing against alternate setups
            ////builder.IntegratedSecurity = false;
            ////builder.UserID = "sa";
            ////builder.Password = "XXX";
            ////builder.DataSource = "127.0.0.1,14330";
            ////builder.DataSource = "20.190.19.170";

            return builder.ToString();
        }

        public static async Task<SemanticVersion> GetCurrentSchemaVersionAsync(
            ITestOutputHelper output,
            string connectionString,
            string schema = DefaultSchema)
        {
            // Returns the latest version as the first result
            string result = (string)await ExecuteSqlAsync(output, $"EXECUTE {schema}._GetVersions", connectionString);
            return SemanticVersion.Parse(result);
        }

        public static async Task<object> ExecuteSqlAsync(ITestOutputHelper output, string commandText, string connectionString = null)
        {
            Exception lastException = null;
            for (int retry = 0; retry < 5; retry++)
            {
                try
                {
                    connectionString ??= GetDefaultConnectionString();
                    await using SqlConnection connection = new SqlConnection(connectionString);
                    await using SqlCommand command = connection.CreateCommand();
                    await command.Connection.OpenAsync();

                    command.CommandText = commandText;
                    output.WriteLine($"Executing SQL command: {commandText}");
                    return await command.ExecuteScalarAsync();
                }
                catch (SqlException e) when (e.Number == 15434)
                {
                    // 15434 : Could not drop login 'XXX' as the user is currently logged in.
                    output.WriteLine($"SQL command failed: {e.Message}");
                    lastException = e;
                }
                catch (SqlException e) when (e.Number == 6106)
                {
                    // 6106 : Process ID 'XXX' is not an active process ID
                    output.WriteLine($"SQL command failed: {e.Message}");
                    lastException = e;
                }
            }

            throw lastException;
        }

        public static async Task InitializeDatabaseAsync(string schema = DefaultSchema)
        {
            var options = new SqlOrchestrationServiceSettings(GetDefaultConnectionString(), schemaName: schema);
            options.CreateDatabaseIfNotExists = true;

            var service = new SqlOrchestrationService(options);
            await service.CreateIfNotExistsAsync();
        }

        public static async Task<TestCredential> CreateTaskHubLoginAsync(ITestOutputHelper output, string prefix, string schema = DefaultSchema)
        {
            // NOTE: Max length for user IDs is 128 characters
            string userId = $"{prefix}_{DateTime.UtcNow:yyyyMMddhhmmssff}";
            string password = GeneratePassword();

            // Generate a low-priviledge user account. This will map to a unique task hub.
            await ExecuteSqlAsync(output, $"CREATE LOGIN [testlogin_{userId}] WITH PASSWORD = '{password}'");
            await ExecuteSqlAsync(output, $"CREATE USER [testuser_{userId}] FOR LOGIN [testlogin_{userId}]");
            await ExecuteSqlAsync(output, $"ALTER ROLE {schema}_runtime ADD MEMBER [testuser_{userId}]");

            var builder = new SqlConnectionStringBuilder(GetDefaultConnectionString())
            {
                UserID = $"testlogin_{userId}",
                Password = password,
                IntegratedSecurity = false,
            };

            return new TestCredential(userId, builder.ToString());
        }

        public static async Task DropTaskHubLoginAsync(ITestOutputHelper output, TestCredential credential, string schema = DefaultSchema)
        {
            // Drop the generated user information
            string userId = credential.UserId;
            await ExecuteSqlAsync(output, $"ALTER ROLE {schema}_runtime DROP MEMBER [testuser_{userId}]");
            await ExecuteSqlAsync(output, $"DROP USER IF EXISTS [testuser_{userId}]");

            // Drop all the connections; otherwise, the DROP LOGIN statement will fail.
            // This is flakey, especially on slower CI machines, so it may need to be retried.
            await ExecuteSqlAsync(output, string.Join(";\n", new string[]
            {
                $"DECLARE @kill varchar(max) = ''",
                $"SELECT @kill = @kill + 'KILL ' + CAST(session_id AS varchar(5)) + ';' FROM sys.dm_exec_sessions WHERE original_login_name = 'testlogin_{userId}'",
                $"EXEC(@kill)",
                $"IF EXISTS (SELECT name FROM sys.sql_logins WHERE name='testlogin_{userId}') DROP LOGIN [testlogin_{userId}]",
            }));
        }

        public static async Task EnableMultiTenancyAsync(ITestOutputHelper output, bool multiTenancy, string schema = DefaultSchema)
        {
            if (!multiTenancy)
            {
                await PurgeAsync(output, schema);
            }

            int param = multiTenancy ? 1 : 0;
            await ExecuteSqlAsync(output, $"EXECUTE {schema}.SetGlobalSetting @Name='TaskHubMode', @Value={param}");
        }

        static string GeneratePassword()
        {
            const string AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPQRSTWXYZ0123456789#$";
            const int PasswordLength = 16;

            string password = GetRandomString(AllowedChars, PasswordLength);
            while (!MeetsSqlPasswordConstraint(password))
            {
                password = GetRandomString(AllowedChars, PasswordLength);
            }

            return password;
        }

        static string GetRandomString(string allowedChars, int length)
        {
            var result = new StringBuilder(length);
            byte[] randomBytes = new byte[length * 4];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(randomBytes);

                for (int i = 0; i < length; i++)
                {
                    int seed = BitConverter.ToInt32(randomBytes, i * 4);
                    Random random = new Random(seed);
                    result.Append(allowedChars[random.Next(allowedChars.Length)]);
                }
            }

            return result.ToString();
        }

        public static DateTime GetCurrentDatabaseTimeUtc()
        {
            string connectionString = GetDefaultConnectionString();
            using SqlConnection connection = new SqlConnection(connectionString);
            using SqlCommand command = connection.CreateCommand();
            command.Connection.Open();

            command.CommandText = "SELECT SYSUTCDATETIME()";
            DateTime currentDatabaseTimeUtc = (DateTime)command.ExecuteScalar();
            return DateTime.SpecifyKind(currentDatabaseTimeUtc, DateTimeKind.Utc);
        }

        static bool MeetsSqlPasswordConstraint(string password)
        {
            return !string.IsNullOrEmpty(password) &&
                password.Any(c => char.IsUpper(c)) &&
                password.Any(c => char.IsLower(c)) &&
                password.Any(c => char.IsDigit(c)) &&
                password.Any(c => !char.IsLetterOrDigit(c)) &&
                password.Length >= 8;
        }


        public static TimeSpan AdjustForDebugging(this TimeSpan timeout)
        {
            if (timeout == default)
            {
                timeout = TimeSpan.FromSeconds(10);
            }

            if (Debugger.IsAttached)
            {
                TimeSpan debuggingTimeout = TimeSpan.FromMinutes(5);
                if (debuggingTimeout > timeout)
                {
                    timeout = debuggingTimeout;
                }
            }

            return timeout;
        }

        public static async Task ParallelForEachAsync<T>(this IEnumerable<T> items, int maxConcurrency, Func<T, Task> action)
        {
            List<Task> tasks;
            if (items is ICollection<T> itemCollection)
            {
                tasks = new List<Task>(itemCollection.Count);
            }
            else
            {
                tasks = new List<Task>();
            }

            using var semaphore = new SemaphoreSlim(maxConcurrency);
            foreach (T item in items)
            {
                tasks.Add(InvokeThrottledAction(item, action, semaphore));
            }

            await Task.WhenAll(tasks);
        }

        static async Task InvokeThrottledAction<T>(T item, Func<T, Task> action, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                await action(item);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static async Task PurgeAsync(ITestOutputHelper output, string schema = "dt")
        {
            await ExecuteSqlAsync(output, $"TRUNCATE TABLE [{schema}].[NewTasks]");
            await ExecuteSqlAsync(output, $"TRUNCATE TABLE [{schema}].[NewEvents]");
            await ExecuteSqlAsync(output, $"TRUNCATE TABLE [{schema}].[Instances]");
            await ExecuteSqlAsync(output, $"TRUNCATE TABLE [{schema}].[History]");
            await ExecuteSqlAsync(output, $"TRUNCATE TABLE [{schema}].[Payloads]");
        }
    }
}
