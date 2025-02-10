// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class SqlDurabilityProvider : DurabilityProvider
    {
        public const string Name = "mssql";

        readonly SqlDurabilityOptions durabilityOptions;
        readonly SqlOrchestrationService service;

        SqlMetricsProvider? singletonSqlMetricsProvider;

        public SqlDurabilityProvider(
            SqlOrchestrationService service,
            SqlDurabilityOptions durabilityOptions)
            : base(Name, service, service, durabilityOptions.ConnectionStringName)
        {
            this.service = service ?? throw new ArgumentNullException(nameof(service));
            this.durabilityOptions = durabilityOptions;
        }
        
        public override bool GuaranteesOrderedDelivery => true;

        public override bool SupportsImplicitEntityDeletion => true;

        public override JObject ConfigurationJson => JObject.FromObject(this.durabilityOptions);

        public override TimeSpan MaximumDelayTime { get; set; } = TimeSpan.MaxValue;

        public override string EventSourceName =>  "DurableTask-SqlServer";

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
                entityJson = JObject.Parse(entityMetadata!);
            }
            catch (JsonException e)
            {
                throw new InvalidDataException($"Unable to read the entity data for {instanceId} because it's in an unrecognizeable format.", e);
            }

            // Entities that are deleted are expected to have { "exists": false }.
            if (entityJson.TryGetValue("exists", out JToken? existsValue) &&
                existsValue.Type == JTokenType.Boolean &&
                existsValue.Value<bool>() == false)
            {
                return null;
            }

            // The actual state comes from the { "state": "..." } string field.
            if (!entityJson.TryGetValue("state", out JToken? value))
            {
                return null;
            }

            return value.ToString();
        }

        public override Task<IList<OrchestrationState>> GetAllOrchestrationStates(CancellationToken cancellationToken)
        {
            throw new NotSupportedException($"Use the method overload that takes a {nameof(OrchestrationStatusQueryCondition)} parameter.");
        }

        public override async Task<OrchestrationStatusQueryResult> GetOrchestrationStateWithPagination(OrchestrationStatusQueryCondition condition, CancellationToken cancellationToken)
        {
            if (condition == null)
            {
                throw new ArgumentNullException(nameof(condition));
            }

            var query = new SqlOrchestrationQuery
            {
                PageSize = condition.PageSize,
                FetchInput = condition.ShowInput,
                CreatedTimeFrom = condition.CreatedTimeFrom,
                CreatedTimeTo = condition.CreatedTimeTo,
                InstanceIdPrefix = condition.InstanceIdPrefix,
            };

            if (condition.RuntimeStatus?.Any() == true)
            {
                query.StatusFilter = new HashSet<OrchestrationStatus>(condition.RuntimeStatus.Select(status => (OrchestrationStatus)status));
            }

            // The continuation token is just a page number.
            if (string.IsNullOrWhiteSpace(condition.ContinuationToken))
            {
                query.PageNumber = 0;
            }
            else if (int.TryParse(condition.ContinuationToken, out int pageNumber))
            {
                query.PageNumber = pageNumber;
            }
            else
            {
                throw new ArgumentException($"The continuation token '{condition.ContinuationToken}' is invalid.", nameof(condition));
            }

            IReadOnlyCollection<OrchestrationState> results = await this.service.GetManyOrchestrationsAsync(query, cancellationToken);

            return new OrchestrationStatusQueryResult
            {
                DurableOrchestrationState = results.Select(s => new DurableOrchestrationStatus
                {
                    Name = s.Name,
                    InstanceId = s.OrchestrationInstance.InstanceId,
                    RuntimeStatus = (OrchestrationRuntimeStatus)s.OrchestrationStatus,
                    CustomStatus = s.Status,
                    CreatedTime = s.CreatedTime,
                    LastUpdatedTime = s.LastUpdatedTime,
                    Input = s.Input,
                    Output = s.Output,
                }),
                ContinuationToken = results.Count == query.PageSize ? (query.PageNumber + 1).ToString() : null,
            };
        }

        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryByInstanceId(string instanceId)
        {
            int deletedInstances = await this.service.PurgeOrchestrationHistoryAsync(new[] { instanceId });
            return new PurgeHistoryResult(deletedInstances);
        }

        /// <summary>
        /// Purges the history of orchestrations and entities based on a set of filters.
        /// </summary>
        /// <remarks>
        /// This method will purge at most 1000 instances. The caller can purge more than this by calling the method
        /// multiple times, checking for a non-zero return value after each call.
        /// </remarks>
        /// <param name="createdTimeFrom">The minimum creation time filter. Only instances created after this date are purged.</param>
        /// <param name="createdTimeTo">The maximum creation time filter. Only instances created before this date are purged.</param>
        /// <param name="runtimeStatus">The set of orchestration status values to filter orchestrations by.</param>
        /// <returns>Returns the number of purged instances.</returns>
        public override async Task<int> PurgeHistoryByFilters(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            var purgeFilter = new PurgeInstanceFilter(createdTimeFrom, createdTimeTo, runtimeStatus);
            var purgeResult = await this.service.PurgeInstanceStateAsync(purgeFilter);
            return purgeResult.DeletedInstanceCount;
        }

        public override async Task RewindAsync(string instanceId, string reason)
        {
            await this.service.RewindTaskOrchestrationAsync(instanceId, reason);
        }

        public override bool TryGetScaleMonitor(
            string functionId,
            string functionName,
            string hubName,
            string storageConnectionString,
            out IScaleMonitor scaleMonitor)
        {
            if (this.singletonSqlMetricsProvider == null)
            {
                this.singletonSqlMetricsProvider = new SqlMetricsProvider(this.service);
            }
            scaleMonitor = new SqlScaleMonitor(hubName, this.singletonSqlMetricsProvider);
            
            return true;
        }

#if FUNCTIONS_V4
        public override bool TryGetTargetScaler(
            string functionId,
            string functionName,
            string hubName,
            string connectionName,
            out ITargetScaler targetScaler)
        {
            if (this.singletonSqlMetricsProvider == null)
            {
                this.singletonSqlMetricsProvider = new SqlMetricsProvider(this.service);
            }

            targetScaler = new SqlTargetScaler(hubName, this.singletonSqlMetricsProvider);
            return true;
        }
#endif
    }
}
