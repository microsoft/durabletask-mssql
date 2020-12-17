namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class SqlDurabilityProvider : DurabilityProvider
    {
        readonly SqlDurabilityOptions options;
        readonly SqlOrchestrationService service;

        public SqlDurabilityProvider(
            SqlOrchestrationService service,
            SqlDurabilityOptions options)
            : base("SQL Server", service, service, options.ConnectionStringName)
        {
            this.options = options;
            this.service = service;
        }

        public SqlDurabilityProvider(
            SqlOrchestrationService service,
            SqlDurabilityOptions options,
            IOrchestrationServiceClient client)
            : base("SQL Server", service, client, options.ConnectionStringName)
        {
            this.options = options;
            this.service = service;
        }

        public override JObject ConfigurationJson => JObject.FromObject(this.options);

        public override async Task<IList<OrchestrationState>> GetOrchestrationStateWithInputsAsync(string instanceId, bool showInput = true)
        {
            OrchestrationState? state = await this.service.GetOrchestrationStateAsync(instanceId, executionId: null);
            if (state == null)
            {
                return Array.Empty<OrchestrationState>();
            }

            if (!showInput)
            {
                // CONSIDER: It would be more efficient to not load the input at all from the data source.
                state.Input = null;
            }

            return new[] { state };
        }

        public override async Task<string?> RetrieveSerializedEntityState(EntityId entityId, JsonSerializerSettings serializierSettings)
        {
            string instanceId = entityId.ToString();
            OrchestrationState? orchestrationState = await this.service.GetOrchestrationStateAsync(
                instanceId,
                executionId: null);

            // Entity state is expected to be persisted as orchestration input.
            string? entityMetadata = orchestrationState?.Input;
            if (string.IsNullOrEmpty(entityMetadata))
            {
                return null;
            }

            // The entity state envelope is expected to be a JSON object.
            JObject entityJson;
            try
            {
                entityJson = JObject.Parse(entityMetadata);
            }
            catch (JsonException e)
            {
                throw new InvalidDataException($"Unable to read the entity data for {instanceId} because it's in an unrecognizeable format.", e);
            }

            // Entities that are deleted are expected to have { "exists": false }.
            if (entityJson.TryGetValue("exists", out JToken existsValue) &&
                existsValue.Type == JTokenType.Boolean &&
                existsValue.Value<bool>() == false)
            {
                return null;
            }

            // The actual state comes from the { "state": "..." } string field.
            if (!entityJson.TryGetValue("state", out JToken value))
            {
                return null;
            }

            return value.ToString();
        }
    }
}
