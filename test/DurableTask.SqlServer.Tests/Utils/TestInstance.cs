// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;

    class TestInstance<TInput>
    {
        readonly TaskHubClient client;
        readonly OrchestrationInstance instance;
        readonly string name;
        readonly string version;
        
        DateTime startTime;
        TInput input;

        public TestInstance(
            TaskHubClient client,
            OrchestrationInstance instance,
            string name,
            string version,
            DateTime startTime,
            TInput input)
        {
            this.client = client;
            this.instance = instance;
            this.name = name;
            this.version = version;
            this.startTime = startTime;
            this.input = input;
        }

        public string InstanceId => this.instance?.InstanceId;

        public string ExecutionId => this.instance?.ExecutionId;

        OrchestrationInstance GetInstanceForAnyExecution() => new OrchestrationInstance
        {
            InstanceId = this.instance.InstanceId,
        };

        public async Task<OrchestrationState> WaitForStart(TimeSpan timeout = default)
        {
            AdjustTimeout(ref timeout);

            Stopwatch sw = Stopwatch.StartNew();
            do
            {
                OrchestrationState state = await this.GetStateAsync();
                if (state != null && state.OrchestrationStatus != OrchestrationStatus.Pending)
                {
                    return state;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));

            } while (sw.Elapsed < timeout);

            throw new TimeoutException($"Orchestration with instance ID '{this.instance.InstanceId}' failed to start.");
        }

        public async Task<OrchestrationState> WaitForCompletion(
            TimeSpan timeout = default,
            OrchestrationStatus expectedStatus = OrchestrationStatus.Completed,
            object expectedOutput = null,
            string expectedOutputRegex = null,
            bool continuedAsNew = false,
            bool doNotAdjustTimeout = false)
        {
            if (!doNotAdjustTimeout)
            {
                AdjustTimeout(ref timeout);
            }

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(this.GetInstanceForAnyExecution(), timeout);
            Assert.NotNull(state);
            Assert.Equal(expectedStatus, state.OrchestrationStatus);

            if (!continuedAsNew)
            {
                if (this.input != null)
                {
                    Assert.Equal(JToken.FromObject(this.input).ToString(), JToken.Parse(state.Input).ToString());
                }
                else
                {
                    Assert.Null(state.Input);
                }
            }

            // For created time, account for potential clock skew
            Assert.True(state.CreatedTime >= this.startTime.AddMinutes(-5));
            Assert.True(state.LastUpdatedTime > state.CreatedTime);
            Assert.True(state.CompletedTime > state.CreatedTime);
            Assert.NotNull(state.OrchestrationInstance);
            Assert.Equal(this.instance.InstanceId, state.OrchestrationInstance.InstanceId);

            // Make sure there is an ExecutionId, but don't require it to match any particular value
            Assert.NotNull(state.OrchestrationInstance.ExecutionId);

            if (expectedOutput != null)
            {
                Assert.NotNull(state.Output);
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

            if (expectedOutputRegex != null)
            {
                Assert.Matches(expectedOutputRegex, state.Output);
            }

            return state;
        }

        internal Task<OrchestrationState> GetStateAsync()
        {
            return this.client.GetOrchestrationStateAsync(new OrchestrationInstance
            {
                InstanceId = this.instance.InstanceId,
            });
        }

        internal Task RaiseEventAsync(string name, object value)
        {
            return this.client.RaiseEventAsync(this.instance, name, value);
        }

        internal Task TerminateAsync(string reason)
        {
            return this.client.TerminateInstanceAsync(this.instance, reason);
        }

        internal async Task RestartAsync(TInput newInput, OrchestrationStatus[] dedupeStatuses = null)
        {
            OrchestrationInstance newInstance = await this.client.CreateOrchestrationInstanceAsync(
                this.name,
                this.version,
                this.InstanceId,
                newInput,
                tags: null,
                dedupeStatuses);

            this.input = newInput;
            this.startTime = DateTime.UtcNow;
            this.instance.ExecutionId = newInstance.ExecutionId;
        }


        internal Task SuspendAsync(string reason = null)
        {
            return this.client.SuspendInstanceAsync(this.instance, reason);
        }

        internal Task ResumeAsync(string reason = null)
        {
            return this.client.ResumeInstanceAsync(this.instance, reason);
        }

        static void AdjustTimeout(ref TimeSpan timeout)
        {
            timeout = timeout.AdjustForDebugging();
        }
    }
}
