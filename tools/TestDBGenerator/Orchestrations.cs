// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TestDBGenerator;

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.SqlServer;

static class Orchestrations
{
    static readonly ManualResetEvent ActivityBlockedEvent = new(initialState: false);
    
    static bool PauseActivityExecution { get; set; } = false;

    public static async Task GenerateRuntimeDataAsync(SqlOrchestrationService service)
    {
        TaskHubWorker worker = new(service);
        worker.AddTaskOrchestrations(
            new SimpleObjectCreator<TaskOrchestration>(nameof(ActivitySequence), typeof(ActivitySequence)),
            new SimpleObjectCreator<TaskOrchestration>(nameof(WaitForSignal), typeof(WaitForSignal)),
            new SimpleObjectCreator<TaskOrchestration>(nameof(WaitForSignalParent), typeof(WaitForSignalParent)));
        worker.AddTaskActivities(
            new SimpleObjectCreator<TaskActivity>(nameof(SayHello), typeof(SayHello)));
        await worker.StartAsync();

        TaskHubClient client = new(service);

        // Schedule one to run to completion
        OrchestrationInstance instance1 = await client.CreateOrchestrationInstanceAsync(
            name: nameof(ActivitySequence),
            version: null,
            instanceId: "Test1",
            input: null);
        await client.WaitForOrchestrationAsync(instance1, TimeSpan.FromSeconds(10));

        // Schedule the second to delay start
        await ScheduleForFutureAsync(
            service,
            name: nameof(ActivitySequence),
            instanceId: "Test2",
            startAt: DateTime.UtcNow.AddMinutes(5));

        // Schedule the third and immediately terminate it
        OrchestrationInstance instance3 = await client.CreateOrchestrationInstanceAsync(
            name: nameof(WaitForSignal),
            version: null,
            instanceId: "Test3",
            input: "signal");
        await WaitForStartAsync(client, instance3, TimeSpan.FromSeconds(10));
        await client.TerminateInstanceAsync(instance3);

        // Schedule the fourth to start but remain uncompleted
        OrchestrationInstance instance4 = await client.CreateOrchestrationInstanceAsync(
            name: nameof(WaitForSignal),
            version: null,
            instanceId: "Test4",
            input: "signal");
        await WaitForStartAsync(client, instance4, TimeSpan.FromSeconds(10));

        // Schedule the fifth to start with an uncompleted sub-orchestration
        OrchestrationInstance instance5 = await client.CreateOrchestrationInstanceAsync(
            name: nameof(WaitForSignalParent),
            version: null,
            instanceId: "Test5",
            input: "signal");
        await WaitForStartAsync(client, instance5, TimeSpan.FromSeconds(10));

        // Schedule the sixth to hang during its activity execution
        PauseActivityExecution = true;
        OrchestrationInstance instance6 = await client.CreateOrchestrationInstanceAsync(
            name: nameof(ActivitySequence),
            version: null,
            instanceId: "Test6",
            input: null);
        if (!ActivityBlockedEvent.WaitOne(TimeSpan.FromSeconds(10)))
        {
            throw new TimeoutException($"Orchestration '{instance6.InstanceId}' hasn't yet signaled to block the activity!");
        }

        await worker.StopAsync();
    }

    static async Task ScheduleForFutureAsync(
        IOrchestrationServiceClient client,
        string name,
        string instanceId,
        DateTime startAt)
    {
        OrchestrationInstance orchestrationInstance = new()
        {
            InstanceId = instanceId,
            ExecutionId = Guid.NewGuid().ToString("N"),
        };

        TaskMessage startMessage = new()
        {
            OrchestrationInstance = orchestrationInstance,
            Event = new ExecutionStartedEvent(-1, null)
            {
                Name = name,
                OrchestrationInstance = orchestrationInstance,
                ScheduledStartTime = startAt
            },
        };

        await client.CreateTaskOrchestrationAsync(startMessage);
    }

    static async Task<OrchestrationState> WaitForStartAsync(
        TaskHubClient client,
        OrchestrationInstance instance,
        TimeSpan timeout)
    {
        Stopwatch sw = Stopwatch.StartNew();
        do
        {
            OrchestrationState state = await client.GetOrchestrationStateAsync(instance);
            if (state != null && state.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                return state;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));

        } while (sw.Elapsed < timeout);

        throw new TimeoutException($"Orchestration with instance ID '{instance.InstanceId}' failed to start.");
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

    class SayHello : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            if (PauseActivityExecution)
            {
                ActivityBlockedEvent.Set();
                Thread.Sleep(Timeout.Infinite);
            }

            return $"Hello, {input}!";
        }
    }

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
