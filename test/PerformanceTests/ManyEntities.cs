// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    class ManyEntities
    {
        const string EntityName = "Counter";

        [JsonProperty("count")]
        public int CurrentValue { get; set; }

        public void Add(int amount) => this.CurrentValue += amount;

        public void Reset() => this.CurrentValue = 0;

        public int Get() => this.CurrentValue;

#pragma warning disable DF0305 // Entity function name must match an existing entity class name.
        [FunctionName(EntityName)]
#pragma warning restore DF0305 // Entity function name must match an existing entity class name.
#pragma warning disable DF0307 // DispatchAsync must be used with the entity name, equal to the name of the function it's used in.
        public static Task Run([EntityTrigger] IDurableEntityContext ctx) => ctx.DispatchAsync<ManyEntities>();
#pragma warning restore DF0307 // DispatchAsync must be used with the entity name, equal to the name of the function it's used in.

        [FunctionName(nameof(StartManyEntities))]
        public static async Task<IActionResult> StartManyEntities(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (!req.TryGetPositiveIntQueryStringParam("entities", out int entities) ||
                !req.TryGetPositiveIntQueryStringParam("messages", out int messages))
            {
                return new BadRequestObjectResult(new
                {
                    error = "Required query string parameters are missing",
                    usage = new
                    {
                        entities = "The number of entities to create",
                        messages = "The number of messages to send to each entity",
                    },
                });
            }

            DateTime utcNow = DateTime.UtcNow;
            string prefix = utcNow.ToString("yyyyMMdd-hhmmss");

            log.LogWarning($"Sending {messages} events to {entities} entities...");

            var tasks = new List<Task>(messages * entities);
            for (int i = 0; i < messages; i++)
            {
                for (int j = 0; j < entities; j++)
                {
                    var entityId = new EntityId(EntityName, $"{prefix}-{j:X16}");
                    tasks.Add(client.SignalEntityAsync(entityId, "add", 1));
                }
            }

            await Task.WhenAll(tasks);

            return new OkObjectResult($"Sent {messages} events to {entities} {EntityName} entities prefixed with '{prefix}'.");
        }
    }
}
