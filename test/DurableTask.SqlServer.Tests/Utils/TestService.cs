// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using Microsoft.Extensions.Logging;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;

    class TestService
    {
        readonly ILoggerFactory loggerFactory;
        readonly string testName;

        TestCredential testCredential;
        TaskHubWorker worker;
        TaskHubClient client;

        public TestService(ITestOutputHelper output)
        {
            this.LogProvider = new TestLogProvider(output);
            this.loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(this.LogProvider);
            });

            this.testName = SharedTestHelpers.GetTestName(output);
            this.OrchestrationServiceOptions = new SqlOrchestrationServiceSettings(
                SharedTestHelpers.GetDefaultConnectionString())
            {
                LoggerFactory = this.loggerFactory,
                CreateDatabaseIfNotExists = true,
            };
        }

        public SqlOrchestrationServiceSettings OrchestrationServiceOptions { get; private set; }

        public Mock<SqlOrchestrationService> OrchestrationServiceMock { get; private set; }

        public TestLogProvider LogProvider { get; }

        public async Task InitializeAsync(bool startWorker = true)
        {
            // The initialization requires administrative credentials (default)
            await new SqlOrchestrationService(this.OrchestrationServiceOptions).CreateIfNotExistsAsync();

            // Enable multitenancy to isolate each test using low-privilege credentials
            await SharedTestHelpers.EnableMultitenancyAsync();

            // The runtime will use low-privilege credentials
            this.testCredential = await SharedTestHelpers.CreateTaskHubLoginAsync(this.testName);
            this.OrchestrationServiceOptions = new SqlOrchestrationServiceSettings(this.testCredential.ConnectionString)
            {
                LoggerFactory = this.loggerFactory,
            };

            // A mock orchestration service allows us to stub out specific methods for testing.
            this.OrchestrationServiceMock = new Mock<SqlOrchestrationService>(this.OrchestrationServiceOptions) { CallBase = true };
            this.worker = new TaskHubWorker(this.OrchestrationServiceMock.Object, this.loggerFactory);
            if (startWorker)
            {
                await this.worker.StartAsync();
            }

            this.client = new TaskHubClient(this.OrchestrationServiceMock.Object, loggerFactory: this.loggerFactory);
        }

        public Task StartWorkerAsync() => this.worker?.StartAsync() ?? Task.CompletedTask;

        public Task PurgeAsync(DateTime maximumThreshold, OrchestrationStateTimeRangeFilterType filterType)
        {
            return this.client.PurgeOrchestrationInstanceHistoryAsync(
                maximumThreshold,
                filterType);
        }

        public async Task DisposeAsync()
        {
            await this.worker.StopAsync(isForced: true);
            this.worker.Dispose();

            await SharedTestHelpers.DropTaskHubLoginAsync(this.testCredential);
        }

        public Task<TestInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            return this.RunOrchestration(input, orchestrationName, string.Empty, implementation, onEvent, activities);
        }

        public Task<TestInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            return this.RunOrchestration(
                input,
                orchestrationName,
                version,
                instanceId: Guid.NewGuid().ToString("N"),
                implementation,
                onEvent,
                activities);
        }

        public async Task<TestInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            string version,
            string instanceId,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            IReadOnlyList<TestInstance<TInput>> instances = await this.RunOrchestrations(
                count: 1,
                instanceIdGenerator: i => instanceId ?? Guid.NewGuid().ToString("N"),
                inputGenerator: i => input,
                orchestrationName: orchestrationName,
                version: version,
                implementation,
                onEvent,
                activities);

            return instances[0];
        }

        public async Task<IReadOnlyList<TestInstance<TInput>>> RunOrchestrations<TOutput, TInput>(
            int count,
            Func<int, string> instanceIdGenerator,
            Func<int, TInput> inputGenerator,
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            // Register the inline orchestration - note that this will only work once per test
            this.RegisterInlineOrchestration(orchestrationName, version, implementation, onEvent);

            foreach ((string name, TaskActivity activity) in activities)
            {
                this.RegisterInlineActivity(name, string.Empty, activity);
            }

            IEnumerable<Task<TestInstance<TInput>>> tasks = Enumerable.Range(0, count).Select(async i =>
            {
                string instanceId = instanceIdGenerator?.Invoke(i) ?? Guid.NewGuid().ToString("N");
                TInput input = inputGenerator != null ? inputGenerator(i) : default;

                DateTime utcNow = DateTime.UtcNow;
                OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationName,
                    version,
                    instanceId,
                    input);

                return new TestInstance<TInput>(
                    this.client,
                    instance,
                    orchestrationName,
                    version,
                    utcNow,
                    input);
            });

            TestInstance<TInput>[] instances = await Task.WhenAll(tasks);
            foreach (TestInstance<TInput> instance in instances)
            {
                // Verify that the CreateOrchestrationInstanceAsync implementation set the InstanceID and ExecutionID fields
                Assert.NotNull(instance.InstanceId);
                Assert.NotNull(instance.ExecutionId);
            }

            return instances;
        }

        public void RegisterInlineActivity(string name, string version, TaskActivity activity)
        {
            this.worker.AddTaskActivities(new TestObjectCreator<TaskActivity>(name, version, activity));
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

        public async Task<string> GetTaskHubNameAsync()
        {
            return (string)await SharedTestHelpers.ExecuteSqlAsync(
                "SELECT dt.CurrentTaskHub()",
                this.testCredential.ConnectionString);
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
                => this.OnEventRaised?.Invoke(context, name, input);
        }

        class TestObjectCreator<T> : ObjectCreator<T>
        {
            readonly T obj;

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
