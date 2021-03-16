// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;

    class TestService
    {
        readonly ILoggerFactory loggerFactory;
        readonly string testName;

        string generatedUserId;
        TaskHubWorker worker;
        TaskHubClient client;

        public TestService(ITestOutputHelper output)
        {
            Type type = output.GetType();
            FieldInfo testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            var test = (ITest)testMember.GetValue(output);

            this.LogProvider = new TestLogProvider(output);
            this.loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(this.LogProvider);
            });

            this.testName = test.TestCase.TestMethod.Method.Name;
            this.OrchestrationServiceOptions = new SqlOrchestrationServiceSettings(
                SharedTestHelpers.GetDefaultConnectionString())
            {
                LoggerFactory = this.loggerFactory,
            };
        }

        public SqlOrchestrationServiceSettings OrchestrationServiceOptions { get; private set; }

        public Mock<SqlOrchestrationService> OrchestrationServiceMock { get; private set; }

        public TestLogProvider LogProvider { get; }

        public async Task InitializeAsync()
        {
            // The initialization requires administrative credentials (default)
            await new SqlOrchestrationService(this.OrchestrationServiceOptions).CreateIfNotExistsAsync();

            // Enable multitenancy to isolate each test using low-privilege credentials
            await this.EnableMultitenancyAsync();

            // The runtime will use low-privilege credentials
            string taskHubConnectionString = await this.CreateTaskHubLoginAsync();
            this.OrchestrationServiceOptions = new SqlOrchestrationServiceSettings(taskHubConnectionString)
            {
                LoggerFactory = this.loggerFactory,
            };

            this.OrchestrationServiceMock = new Mock<SqlOrchestrationService>(this.OrchestrationServiceOptions) { CallBase = true };
            this.worker = await new TaskHubWorker(this.OrchestrationServiceMock.Object, this.loggerFactory).StartAsync();
            this.client = new TaskHubClient(this.OrchestrationServiceMock.Object, loggerFactory: this.loggerFactory);
        }

        public Task PurgeAsync(DateTime minimumThreshold, OrchestrationStateTimeRangeFilterType filterType)
        {
            return this.client.PurgeOrchestrationInstanceHistoryAsync(
                minimumThreshold,
                filterType);
        }

        public async Task DisposeAsync()
        {
            await this.worker.StopAsync(isForced: true);
            this.worker.Dispose();

            await this.DropTaskHubLoginAsync();
        }

        public Task<TestInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities) =>
            RunOrchestration(input, orchestrationName, string.Empty, implementation, onEvent, activities);

        public async Task<TestInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            var instances = await this.RunOrchestrations(
                count: 1,
                inputGenerator: i => input,
                orchestrationName: orchestrationName,
                version: version,
                implementation,
                onEvent,
                activities);

            return instances.First();
        }

        public async Task<List<TestInstance<TInput>>> RunOrchestrations<TOutput, TInput>(
            int count,
            Func<int, TInput> inputGenerator,
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            // Register the inline orchestration - note that this will only work once per test
            RegisterInlineOrchestration(orchestrationName, version, implementation, onEvent);

            foreach ((string name, TaskActivity activity) in activities)
            {
                this.worker.AddTaskActivities(new TestObjectCreator<TaskActivity>(name, activity));
            }

            DateTime utcNow = DateTime.UtcNow;

            var instances = new List<TestInstance<TInput>>(count);
            for (int i = 0; i < count; i++)
            {
                TInput input = inputGenerator(i);
                OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationName,
                    version,
                    input);

                // Verify that the CreateOrchestrationInstanceAsync implementation set the InstanceID and ExecutionID fields
                Assert.NotNull(instance.InstanceId);
                Assert.NotNull(instance.ExecutionId);

                instances.Add(new TestInstance<TInput>(this.client, instance, utcNow, input));
            }

            return instances;
        }

        public void RegisterInlineOrchestration<TOutput, TInput>(
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null)
        {
            this.worker.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(
                orchestrationName,
                version,
                MakeOrchestration(implementation, onEvent)));
        }

        public static TaskOrchestration MakeOrchestration<TOutput, TInput>(
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null)
        {
            return new OrchestrationShim<TOutput, TInput>(implementation, onEvent);
        }

        // This is just a wrapper around the constructor for convenience. It allows us to write 
        // less code because generic arguments for methods can be implied, unlike constructors.
        public static TaskActivity MakeActivity<TInput, TOutput>(
            Func<TaskContext, TInput, TOutput> implementation)
        {
            return new ActivityShim<TInput, TOutput>(implementation);
        }

        static string GetFriendlyTypeName(Type type)
        {
            string friendlyName = type.Name;
            if (type.IsGenericType)
            {
                int iBacktick = friendlyName.IndexOf('`');
                if (iBacktick > 0)
                {
                    friendlyName = friendlyName.Remove(iBacktick);
                }

                friendlyName += "<";
                Type[] typeParameters = type.GetGenericArguments();
                for (int i = 0; i < typeParameters.Length; ++i)
                {
                    string typeParamName = GetFriendlyTypeName(typeParameters[i]);
                    friendlyName += (i == 0 ? typeParamName : "," + typeParamName);
                }

                friendlyName += ">";
            }

            return friendlyName;
        }

        public IEnumerable<LogEntry> GetAndValidateLogs()
        {
            if (this.LogProvider.TryGetLogs("DurableTask.SqlServer", out IReadOnlyCollection<LogEntry> logs))
            {
                foreach (LogEntry entry in logs)
                {
                    LogAssert.ValidateStructuredLogFields(entry);
                    yield return entry;
                }
            }
        }

        internal Task EnableMultitenancyAsync()
        {
            return ExecuteCommandAsync($"EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=1");
        }

        internal async Task<string> CreateTaskHubLoginAsync()
        {
            // NOTE: Max length for user IDs is 128 characters
            string userId = $"{this.testName}_{DateTime.UtcNow:yyyyMMddhhmmssff}";
            string password = GeneratePassword();

            // Generate a low-priviledge user account. This will map to a unique task hub.
            await ExecuteCommandAsync($"CREATE LOGIN [testlogin_{userId}] WITH PASSWORD = '{password}'");
            await ExecuteCommandAsync($"CREATE USER [testuser_{userId}] FOR LOGIN [testlogin_{userId}]");
            await ExecuteCommandAsync($"ALTER ROLE dt_runtime ADD MEMBER [testuser_{userId}]");

            var existing = new SqlConnectionStringBuilder(this.OrchestrationServiceOptions.TaskHubConnectionString);
            var builder = new SqlConnectionStringBuilder()
            {
                UserID = $"testlogin_{userId}",
                Password = password,
                DataSource = existing.DataSource,
                InitialCatalog = existing.InitialCatalog,
            };

            this.generatedUserId = userId;
            return builder.ToString();
        }

        async Task DropTaskHubLoginAsync()
        {
            // Drop the generated user information
            string userId = this.generatedUserId;
            await ExecuteCommandAsync($"ALTER ROLE dt_runtime DROP MEMBER [testuser_{userId}]");
            await ExecuteCommandAsync($"DROP USER IF EXISTS [testuser_{userId}]");

            // drop all the connections; otherwise, the DROP LOGIN statement will fail
            await ExecuteCommandAsync($"DECLARE @kill varchar(max) = ''; SELECT @kill = @kill + 'KILL ' + CAST(session_id AS varchar(5)) + ';' FROM sys.dm_exec_sessions WHERE original_login_name = 'testlogin_{userId}'; EXEC(@kill);");
            await ExecuteCommandAsync($"DROP LOGIN [testlogin_{userId}]");
        }

        static async Task ExecuteCommandAsync(string commandText)
        {
            for (int retry = 0; retry < 3; retry++)
            {
                try
                {
                    string connectionString = SharedTestHelpers.GetDefaultConnectionString();
                    await using SqlConnection connection = new SqlConnection(connectionString);
                    await using SqlCommand command = connection.CreateCommand();
                    await command.Connection.OpenAsync();

                    command.CommandText = commandText;
                    await command.ExecuteNonQueryAsync();
                    break;
                }
                catch (SqlException e) when (e.Number == 15434)
                {
                    // 15434 : Could not drop login 'XXX' as the user is currently logged in.
                }
            }
        }

        static string GeneratePassword()
        {
            const string AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNPQRSTWXYZ0123456789#$";
            const int PasswordLenth = 16;

            string password = GetRandomString(AllowedChars, PasswordLenth);
            while (!MeetsSqlPasswordConstraint(password))
            {
                password = GetRandomString(AllowedChars, PasswordLenth);
            }

            return password;
        }

        static string GetRandomString(string allowedChars, int length)
        {
            var result = new StringBuilder(length);
            byte[] randomBytes = new byte[length * 4];
            using (var rng = new RNGCryptoServiceProvider())
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

        static bool MeetsSqlPasswordConstraint(string password)
        {
            return !string.IsNullOrEmpty(password) &&
                password.Any(c => char.IsUpper(c)) &&
                password.Any(c => char.IsLower(c)) &&
                password.Any(c => char.IsDigit(c)) &&
                password.Any(c => !char.IsLetterOrDigit(c)) &&
                password.Length >= 8;
        }

        class ActivityShim<TInput, TOutput> : TaskActivity<TInput, TOutput>
        {
            public ActivityShim(Func<TaskContext, TInput, TOutput> implementation)
            {
                this.Implementation = implementation;
            }

            public Func<TaskContext, TInput, TOutput> Implementation { get; }

            protected override TOutput Execute(TaskContext context, TInput input)
            {
                return this.Implementation(context, input);
            }
        }

        class OrchestrationShim<TOutput, TInput> : TaskOrchestration<TOutput, TInput>
        {
            public OrchestrationShim(
                Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
                Action<OrchestrationContext, string, string> onEvent = null)
            {
                this.Implementation = implementation;
                this.OnEventRaised = onEvent;
            }

            public Func<OrchestrationContext, TInput, Task<TOutput>> Implementation { get; set; }

            public Action<OrchestrationContext, string, string> OnEventRaised { get; set; }

            public override Task<TOutput> RunTask(OrchestrationContext context, TInput input)
                => this.Implementation(context, input);

            public override void RaiseEvent(OrchestrationContext context, string name, string input)
                => this.OnEventRaised(context, name, input);
        }

        class TestObjectCreator<T> : ObjectCreator<T>
        {
            readonly T obj;

            public TestObjectCreator(string name, T obj) : this(name, string.Empty, obj)
            {
            }

            public TestObjectCreator(string name, string version, T obj)
            {
                this.Name = name;
                this.Version = version;
                this.obj = obj;
            }

            public override T Create() => this.obj;
        }
    }
}
