// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;

    class ManyMixedOrchestrations
    {
        [FunctionName(nameof(StartManyMixedOrchestrations))]
        public static async Task<IActionResult> StartManyMixedOrchestrations(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient starter,
            ILogger log)
        {
            if (!int.TryParse(req.Query["count"], out int count) || count < 1)
            {
                return new BadRequestObjectResult("A 'count' query string parameter is required and it must contain a positive number.");
            }

            string prefix = await Common.ScheduleManyInstances(starter, nameof(MixedOrchestration), count, log);
            return new OkObjectResult($"Scheduled {count} orchestrations prefixed with '{prefix}'.");
        }

        [FunctionName(nameof(MixedOrchestration))]
        public static async Task MixedOrchestration(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            // Flow:
            // 1. Call an activity function
            // 2. Start a sub-orchestration
            // 3. Wait for an external event w/a timer + sub-orchestration completion
            // 4. Call an activity with a retry policy and catch the exception
            await context.CallActivityAsync(nameof(Common.SayHello), "World");

            string callbackEventName = "CallbackEvent";
            Task subOrchestration = context.CallSubOrchestratorAsync(
                nameof(CallMeBack),
                context.InstanceId + "-sub",
                (context.InstanceId, callbackEventName));

            Task onCalledBack = context.WaitForExternalEvent(callbackEventName, TimeSpan.FromMinutes(1));

            await Task.WhenAll(subOrchestration, onCalledBack);

            try
            {
                await context.CallActivityWithRetryAsync(
                    nameof(Throw),
                    new RetryOptions(TimeSpan.FromSeconds(5), 2),
                    null);
            }
            catch (FunctionFailedException)
            {
                // no-op
            }
        }

        [FunctionName(nameof(CallMeBack))]
        public static Task CallMeBack([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            (string callbackInstance, string eventName) = context.GetInput<(string, string)>();
            return context.CallActivityAsync(
                nameof(RaiseEvent),
                input: (callbackInstance, eventName));
        }

        [FunctionName(nameof(RaiseEvent))]
        public static Task RaiseEvent(
            [ActivityTrigger] (string instanceId, string eventName) input,
            [DurableClient] IDurableClient client)
        {
            return client.RaiseEventAsync(input.instanceId, input.eventName);
        }

        [FunctionName(nameof(Throw))]
        public static void Throw([ActivityTrigger] IDurableActivityContext ctx) => throw new Exception("Kah-BOOOOM!!!");
    }
}
