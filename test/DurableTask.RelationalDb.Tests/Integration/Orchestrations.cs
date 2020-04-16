namespace DurableTask.RelationalDb.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.RelationalDb.Tests.Logging;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    public class Orchestrations : IAsyncLifetime
    {
        readonly SqlServerProviderOptions options;

        TaskHubWorker worker;
        TaskHubClient client;

        public Orchestrations(ITestOutputHelper output)
        {
            var logProvider = new TestLogProvider(output);
            this.options = new SqlServerProviderOptions
            {
                LoggerFactory = LoggerFactory.Create(builder => builder.AddProvider(logProvider)),
            };
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
            var provider = new SqlServerOrchestrationService(this.options);
            this.worker = await new TaskHubWorker(provider).StartAsync();
            this.client = new TaskHubClient(provider);
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

            // Does nothing except return the original input
            StartedInstance<string> instance = await this.RunOrchestration(
                input,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(
                expectedOutput: input);
        }

        [Fact]
        public async Task OrchestrationWithTimer_Completes()
        {
            string input = $"Hello {DateTime.UtcNow:o}";
            TimeSpan delay = TimeSpan.FromSeconds(3);

            // Performs a delay and then returns the input
            StartedInstance<string> instance = await this.RunOrchestration(
                input,
                implementation: (ctx, input) => ctx.CreateTimer(ctx.CurrentUtcDateTime.Add(delay), input));

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: input);

            // Verify that the delay actually happened
            Assert.True(state.CreatedTime.Add(delay) <= state.CompletedTime);
        }


        [Fact]
        public async Task Orchestration_IsReplaying_Works()
        {
            StartedInstance<string> instance = await this.RunOrchestration<List<bool>, string>(
                null,
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
                implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                activities: new[] {
                    ("SayHello", MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")),
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: $"Hello, {input}!");
        }

        [Fact]
        public async Task OrchestrationWithActivityChain_Completes()
        {
            StartedInstance<string> instance = await this.RunOrchestration<int, string>(
                null,
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

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: 10);
        }

        [Fact]
        public async Task OrchestrationWithException_Fails()
        {
            string errorMessage = "Kah-BOOOOOM!!!";

            // The exception is expected to fail the orchestration execution
            StartedInstance<string> instance = await this.RunOrchestration<string, string>(
                null,
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

        async Task<StartedInstance<TInput>> RunOrchestration<TOutput, TInput>(
            TInput input,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            params (string name, TaskActivity activity)[] activities)
        {
            // Register the inline orchestration - note that this will only work once per test
            Type orchestrationType = typeof(TestOrchestration<TOutput, TInput>);
            this.worker.AddTaskOrchestrations(orchestrationType);

            foreach ((string name, TaskActivity activity) in activities)
            {
                this.worker.AddTaskActivities(new TestObjectCreator<TaskActivity>(name, activity));
            }

            DateTime utcNow = DateTime.UtcNow;

            // This static property is used to store the orchestration's implementation.
            // This will only work for one orchestration at a time, so tests must run serially.
            TestOrchestration<TOutput, TInput>.Implementation = implementation;
            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                orchestrationType,
                input);

            // Verify that the CreateOrchestrationInstanceAsync implementation set the InstanceID and ExecutionID fields
            Assert.NotNull(instance.InstanceId);
            Assert.NotNull(instance.ExecutionId);

            return new StartedInstance<TInput>(this.client, instance, utcNow, input);
        }

        // This is just a wrapper around the constructor for convenience. It allows us to write 
        // less code because generic arguments for methods can be implied, unlike constructors.
        static TaskActivity MakeActivity<TInput, TOutput>(
            Func<TaskContext, TInput, TOutput> implementation)
        {
            return new ActivityShim<TInput, TOutput>(implementation);
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

                Assert.Equal(expectedStatus, state?.OrchestrationStatus);

                if (this.input != null)
                {
                    Assert.Equal(JToken.FromObject(this.input), JToken.Parse(state.Input));
                }
                else
                {
                    Assert.Null(state.Input);
                }

                Assert.True(state.CreatedTime >= this.startTime);
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
