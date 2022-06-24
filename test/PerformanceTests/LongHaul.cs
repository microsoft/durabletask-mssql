// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;

    public class LongHaul
    {
        // HTTPie command:
        // http post http://localhost:7071/api/StartLongHaul TotalHours:=1 OrchestrationsPerInterval:=1000 Interval=00:05:00
        [FunctionName(nameof(StartLongHaul))]
        public static async Task<IActionResult> StartLongHaul(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            string input = await req.ReadAsStringAsync();

            LongHaulOptions options = null;
            if (!string.IsNullOrEmpty(input))
            {
                try
                {
                    options = JsonSerializer.Deserialize<LongHaulOptions>(input);
                }
                catch (JsonException e)
                {
                    log.LogWarning(e, "Received bad JSON input");
                }
            }

            if (options == null || !options.IsValid())
            {
                return new BadRequestObjectResult(new
                {
                    error = "Required request content is missing or invalid",
                    usage = new SortedDictionary<string, string>
                    {
                        [nameof(LongHaulOptions.TotalHours)] = "The total length of time the test should run. Example: '72' for 72 hours.",
                        [nameof(LongHaulOptions.OrchestrationsPerInterval)] = "The number of orchestrations to schedule per interval. Example: '1000' to schedule 1,000 every interval.",
                        [nameof(LongHaulOptions.Interval)] = "The frequency for scheduling orchestration batches. Example: '00:05:00' for 5 minutes.",
                    },
                });
            }

            // Instance ID contains the timestamp and the configuration parameters for easier searching and categorization
            string instanceId = $"longhaul_{DateTime.UtcNow:yyyyMMddHHmmss}_{options.TotalHours}_{options.OrchestrationsPerInterval}_{(int)options.Interval.TotalSeconds}";
            await starter.StartNewAsync(
                nameof(LongHaulOrchestrator),
                instanceId,
                new LongHaulState 
                { 
                    Options = options,
                    Deadline = DateTime.UtcNow.AddHours(options.TotalHours),
                });

            log.LogWarning("Started long-haul orchestrator with ID = {instanceId}", instanceId);
            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        /// <summary>
        /// Long-running orchestration that schedules bursts of shorter "Hello cities" orchestrations to run on a given interval
        /// </summary>
        [FunctionName(nameof(LongHaulOrchestrator))]
        public static async Task LongHaulOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            LongHaulState state = context.GetInput<LongHaulState>();
            if (context.CurrentUtcDateTime > state.Deadline)
            {
                return;
            }

            state.Iteration++;
            int currentTotal = state.TotalOrchestrationsCompleted;

            // Schedule all orchestrations in parallel
            List<Task> tasks = Enumerable
                .Range(0, state.Options.OrchestrationsPerInterval)
                .Select(i =>
                {
                    // Each sub-orchestration should have a unique ID
                    int suffix = state.TotalOrchestrationsCompleted + i;
                    return context.CallSubOrchestratorAsync(
                        nameof(ManySequences.HelloCities),
                        instanceId: $"{context.InstanceId}_{suffix:X8}",
                        input: null);
                })
                .ToList();

            context.SetCustomStatus(state);

            // Wait for all sub-orchestrations to complete
            await Task.WhenAll(tasks);

            state.TotalOrchestrationsCompleted += tasks.Count;
            context.SetCustomStatus(state);

            DateTime nextRunTime = context.CurrentUtcDateTime.Add(state.Options.Interval);
            await context.CreateTimer(nextRunTime, CancellationToken.None);

            context.ContinueAsNew(state);
        }

        /// <summary>
        /// Options for starting this orchestration
        /// </summary>
        class LongHaulOptions
        {
            [Newtonsoft.Json.JsonProperty]
            public int TotalHours { get; set; }
            [Newtonsoft.Json.JsonProperty]
            public int OrchestrationsPerInterval { get; set; }
            [Newtonsoft.Json.JsonProperty]
            public TimeSpan Interval { get; set; }

            public bool IsValid() => 
                this.TotalHours > 0 &&
                this.OrchestrationsPerInterval > 0 &&
                this.Interval > TimeSpan.Zero;
        }

        /// <summary>
        /// State maintained by the long-haul orchestration
        /// </summary>
        class LongHaulState
        {
            [Newtonsoft.Json.JsonProperty]
            public LongHaulOptions Options { get; set; }

            [Newtonsoft.Json.JsonProperty]
            public DateTime Deadline { get; set; }

            [Newtonsoft.Json.JsonProperty(DefaultValueHandling = Newtonsoft.Json.DefaultValueHandling.Ignore)]
            public int TotalOrchestrationsCompleted { get; set; }

            [Newtonsoft.Json.JsonProperty(DefaultValueHandling = Newtonsoft.Json.DefaultValueHandling.Ignore)]
            public int Iteration { get; set; }
        }
    }
}
