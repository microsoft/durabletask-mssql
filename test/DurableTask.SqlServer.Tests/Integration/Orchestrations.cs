namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Logging;
    using DurableTask.SqlServer.Tests.Logging;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    public class Orchestrations : IAsyncLifetime
    {
        readonly SqlServerProviderOptions options;
        readonly TestLogProvider logProvider;
        readonly ILoggerFactory loggerFactory;

        TaskHubWorker worker;
        TaskHubClient client;

        public Orchestrations(ITestOutputHelper output)
        {
            Type type = output.GetType();
            FieldInfo testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            var test = (ITest)testMember.GetValue(output);
            
            this.logProvider = new TestLogProvider(output);
            this.loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(this.logProvider);
            });

            this.options = new SqlServerProviderOptions
            {
                AppName = test.DisplayName,
                LoggerFactory = this.loggerFactory,
            };
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
            var provider = new SqlServerOrchestrationService(this.options);
            await ((IOrchestrationService)provider).CreateIfNotExistsAsync();

            this.worker = await new TaskHubWorker(provider, this.loggerFactory).StartAsync();
            this.client = new TaskHubClient(provider, loggerFactory: this.loggerFactory);
        }

        async Task IAsyncLifetime.DisposeAsync()
        {
            await this.worker.StopAsync(isForced: true);
            this.worker.Dispose();
        }

        [Fact]
        public async Task EmptyOrchestration_Completes()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "EmptyOrchestration";

            // Does nothing except return the original input
            StartedInstance<string> instance = await this.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(
                expectedOutput: input);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.CheckpointingOrchestration(orchestrationName));
        }

        [Fact]
        public async Task OrchestrationWithTimer_Completes()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            string orchestrationName = "OrchestrationWithTimer";
            TimeSpan delay = TimeSpan.FromSeconds(3);

            // Performs a delay and then returns the input
            StartedInstance<string> instance = await this.RunOrchestration(
                input,
                orchestrationName,
                implementation: (ctx, input) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input));

            TimeSpan timeout = TimeSpan.FromSeconds(10);
            OrchestrationState state = await instance.WaitForCompletion(
                timeout,
                expectedOutput: input);

            // Verify that the delay actually happened
            Assert.True(state.CreatedTime.Add(delay) <= state.CompletedTime);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.logProvider);
            LogAssert.Sequence(
                this.logProvider,
                LogAssert.AcquiredAppLock(),
                LogAssert.CheckpointingOrchestration(orchestrationName),
                LogAssert.CheckpointingOrchestration(orchestrationName));
        }

        [Fact]
        public async Task Orchestration_IsReplaying_Works()
        {
            StartedInstance<string> instance = await this.RunOrchestration<List<bool>, string>(
                null,
                orchestrationName: "TwoTimerReplayTester",
                implementation: async (ctx, _) =>
                {
                    var list = new List<bool>();
                    list.Add(ctx.IsReplaying);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime, 0);
                    list.Add(ctx.IsReplaying);
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime, 0);
                    list.Add(ctx.IsReplaying);
                    return list;
                });

            OrchestrationState state = await instance.WaitForCompletion();
            JArray results = JArray.Parse(state.Output);
            Assert.Equal(3, results.Count);
            Assert.True((bool)results[0]);
            Assert.True((bool)results[1]);
            Assert.False((bool)results[2]);
        }

        [Fact]
        public async Task OrchestrationWithActivity_Completes()
        {
            string input = $"[{DateTime.UtcNow:o}]";

            StartedInstance<string> instance = await this.RunOrchestration(
                input,
                orchestrationName: "OrchestrationWithActivity",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                activities: new[] {
                    ("SayHello", MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: $"Hello, {input}!");
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        public async Task OrchestrationsWithActivityChain_Completes(int parallelCount)
        {
            List<StartedInstance<string>> instances = await this.RunOrchestrations<int, string>(
                parallelCount,
                _ => null,
                orchestrationName: "OrchestrationsWithActivityChain",
                implementation: async (ctx, _) =>
                {
                    int value = 0;
                    for (int i = 0; i < 10; i++)
                    {
                        value = await ctx.ScheduleTask<int>("PlusOne", "", value);
                    }

                    return value;
                },
                activities: new[] {
                    ("PlusOne", MakeActivity((TaskContext ctx, int input) => input + 1)),
                });

            IEnumerable<Task> tasks = instances.Select(
                instance => instance.WaitForCompletion(
                    timeout: TimeSpan.FromSeconds(30),
                    expectedOutput: 10));
            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task OrchestrationWithException_Fails()
        {
            string errorMessage = "Kah-BOOOOOM!!!";

            // The exception is expected to fail the orchestration execution
            StartedInstance<string> instance = await this.RunOrchestration<string, string>(
                null,
                orchestrationName: "OrchestrationWithException",
                implementation: (ctx, input) => throw new Exception(errorMessage));

            await instance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(10),
                expectedOutput: errorMessage,
                expectedStatus: OrchestrationStatus.Failed);
        }

        [Fact]
        public async Task OrchestrationWithActivityFailure_Fails()
        {
            // Performs a delay and then returns the input
            StartedInstance<string> instance = await this.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Throw", ""),
                activities: new[] {
                    ("Throw", MakeActivity<string, string>((ctx, input) => throw new Exception("Kah-BOOOOOM!!!"))),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedStatus: OrchestrationStatus.Failed,
                expectedOutput: null); // TODO: Test for error message in output
        }

        [Fact]
        public async Task OrchestrationWithActivityFanOut()
        {
            StartedInstance<string> instance = await this.RunOrchestration<string[], string>(
                null,
                orchestrationName: "OrchestrationWithActivityFanOut",
                implementation: async (ctx, _) =>
                {
                    var tasks = new List<Task<string>>();
                    for (int i = 0; i < 10; i++)
                    {
                        tasks.Add(ctx.ScheduleTask<string>("ToString", "", i));
                    }

                    string[] results = await Task.WhenAll(tasks);
                    Array.Sort(results);
                    Array.Reverse(results);
                    return results;
                },
                activities: new[] {
                    ("ToString", MakeActivity((TaskContext ctx, int input) => input.ToString())),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: new[] { "9", "8", "7", "6", "5", "4", "3", "2", "1", "0" });

        }

        async Task<List<StartedInstance<TInput>>> RunOrchestrations<TOutput, TInput>(
            int count,
            Func<int, TInput> inputGenerator,
            string orchestrationName,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            params (string name, TaskActivity activity)[] activities)
        {
            // Register the inline orchestration - note that this will only work once per test
            Type orchestrationType = typeof(TestOrchestration<TOutput, TInput>);
            
            this.worker.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(
                orchestrationName,
                (TaskOrchestration)Activator.CreateInstance(orchestrationType)));

            foreach ((string name, TaskActivity activity) in activities)
            {
                this.worker.AddTaskActivities(new TestObjectCreator<TaskActivity>(name, activity));
            }

            DateTime utcNow = DateTime.UtcNow;

            // This static property is used to store the orchestration's implementation.
            // This will only work for one orchestration at a time, so tests must run serially.
            TestOrchestration<TOutput, TInput>.Implementation = implementation;

            var instances = new List<StartedInstance<TInput>>(count);
            for (int i = 0; i < count; i++)
            {
                TInput input = inputGenerator(i);
                OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationName,
                    string.Empty /* version */,
                    input);

                // Verify that the CreateOrchestrationInstanceAsync implementation set the InstanceID and ExecutionID fields
                Assert.NotNull(instance.InstanceId);
                Assert.NotNull(instance.ExecutionId);

                instances.Add(new StartedInstance<TInput>(this.client, instance, utcNow, input));
            }
            
            return instances;
        }

        async Task<StartedInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            params (string name, TaskActivity activity)[] activities)
        {
            var instances = await this.RunOrchestrations(
                count: 1,
                inputGenerator: i => input,
                orchestrationName,
                implementation,
                activities);

            return instances.First();
        }

        // This is just a wrapper around the constructor for convenience. It allows us to write 
        // less code because generic arguments for methods can be implied, unlike constructors.
        static TaskActivity MakeActivity<TInput, TOutput>(
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

        class TestOrchestration<TOutput, TInput> : TaskOrchestration<TOutput, TInput>
        {
            public static Func<OrchestrationContext, TInput, Task<TOutput>> Implementation { get; set; }

            public override Task<TOutput> RunTask(OrchestrationContext context, TInput input)
                => Implementation(context, input);
        }

        class StartedInstance<T>
        {
            readonly TaskHubClient client;
            readonly OrchestrationInstance instance;
            readonly DateTime startTime;
            readonly T input;

            public StartedInstance(
                TaskHubClient client,
                OrchestrationInstance instance,
                DateTime startTime,
                T input)
            {
                this.client = client;
                this.instance = instance;
                this.startTime = startTime;
                this.input = input;
            }

            public async Task<OrchestrationState> WaitForCompletion(
                TimeSpan timeout = default,
                OrchestrationStatus expectedStatus = OrchestrationStatus.Completed,
                object expectedOutput = null)
            {
                if (timeout == default)
                {
                    timeout = TimeSpan.FromSeconds(5);
                }

                if (Debugger.IsAttached)
                {
                    timeout = timeout.Add(TimeSpan.FromMinutes(5));
                }

                OrchestrationState state = await this.client.WaitForOrchestrationAsync(this.instance, timeout);
                Assert.NotNull(state);
                Assert.Equal(expectedStatus, state.OrchestrationStatus);

                if (this.input != null)
                {
                    Assert.Equal(JToken.FromObject(this.input), JToken.Parse(state.Input));
                }
                else
                {
                    Assert.Null(state.Input);
                }

                // For created time, account for potential clock skew
                Assert.True(state.CreatedTime >= this.startTime.AddMinutes(-5));
                Assert.True(state.LastUpdatedTime > state.CreatedTime);
                Assert.True(state.CompletedTime > state.CreatedTime);
                Assert.NotNull(state.OrchestrationInstance);
                Assert.Equal(this.instance.InstanceId, state.OrchestrationInstance.InstanceId);
                Assert.Equal(this.instance.ExecutionId, state.OrchestrationInstance.ExecutionId);

                if (expectedOutput != null)
                {
                    try
                    {
                        // DTFx usually encodes outputs as JSON values. The exception is error messages.
                        // If this is an error message, we'll throw here and try the logic in the catch block.
                        JToken.Parse(state.Output);
                        Assert.Equal(JToken.FromObject(expectedOutput).ToString(Formatting.None), state.Output);
                    }
                    catch (JsonReaderException)
                    {
                        Assert.Equal(expectedOutput, state?.Output);
                    }
                }

                return state;
            }
        }

        class TestObjectCreator<T> : ObjectCreator<T>
        {
            readonly T obj;

            public TestObjectCreator(string name, T obj)
            {
                this.Name = name;
                this.Version = string.Empty;
                this.obj = obj;
            }

            public override T Create() => this.obj;
        }
    }
}
