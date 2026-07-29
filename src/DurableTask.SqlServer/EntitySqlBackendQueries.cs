// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Newtonsoft.Json.Linq;

    class EntitySqlBackendQueries : EntityBackendQueries
    {
        readonly SqlOrchestrationService orchestrationService;

        static readonly TimeSpan TimeLimitForCleanEntityStorageLoop = TimeSpan.FromSeconds(5);

        public EntitySqlBackendQueries(
            SqlOrchestrationService orchestrationService)
        {
            this.orchestrationService = orchestrationService;
        }

        public async override Task<EntityMetadata?> GetEntityAsync(
            EntityId id,
            bool includeState = false,
            bool includeStateless = false,
            CancellationToken cancellation = default)
        {
            OrchestrationState? state = (await this.orchestrationService.GetOrchestrationStateAsync(id.ToString(), allExecutions: false)).FirstOrDefault();
            return this.GetEntityMetadata(state, includeStateless, includeState);
        }

        public async override Task<EntityQueryResult> QueryEntitiesAsync(EntityQuery filter, CancellationToken cancellation)
        {
            int pageNumber = 0;
            if (!string.IsNullOrEmpty(filter.ContinuationToken) && !int.TryParse(filter.ContinuationToken, out pageNumber))
            {
                throw new ArgumentException($"Invalid continuation token {filter.ContinuationToken}");
            }

            int retrievedResults = 0;
            IEnumerable<OrchestrationState> allResults = Array.Empty<OrchestrationState>();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            do
            {
                SqlOrchestrationQuery entityInstancesQuery = new SqlOrchestrationQuery()
                {
                    PageSize = filter.PageSize.GetValueOrDefault(100),
                    PageNumber = pageNumber,
                    InstanceIdPrefix = filter.InstanceIdStartsWith ?? "@",
                    CreatedTimeFrom = filter.LastModifiedFrom.GetValueOrDefault(DateTime.MinValue),
                    CreatedTimeTo = filter.LastModifiedTo.GetValueOrDefault(DateTime.MaxValue),
                    FetchInput = filter.IncludeState,
                };
                IReadOnlyCollection<OrchestrationState> results = await this.orchestrationService.GetManyOrchestrationsAsync(entityInstancesQuery, cancellation);
                allResults = allResults.Concat(results);
                pageNumber++;

                retrievedResults = results.Count;
                if (retrievedResults == 0)
                {
                    pageNumber = -1;
                }
            } while (retrievedResults > 0 && stopwatch.ElapsedMilliseconds <= 100);

            IEnumerable<EntityMetadata> entities = allResults.Select(result => this.GetEntityMetadata(result, filter.IncludeTransient, filter.IncludeState))
                                                   .OfType<EntityMetadata>();

            return new EntityQueryResult()
            {
                Results = entities,
                ContinuationToken = pageNumber < 0 ? null : pageNumber.ToString()
            };
        }

        public async override Task<CleanEntityStorageResult> CleanEntityStorageAsync(CleanEntityStorageRequest request = default, CancellationToken cancellation = default)
        {
            DateTime now = DateTime.UtcNow;
            int emptyEntitiesRemoved = 0;
            int orphanedLocksReleased = 0;
            int pageNumber = 0;
            if (!string.IsNullOrEmpty(request.ContinuationToken) && !int.TryParse(request.ContinuationToken, out pageNumber))
            {
                throw new ArgumentException($"Invalid continuation token {request.ContinuationToken}");
            }

            int retrievedResults = 0;
            IEnumerable<OrchestrationState> allResults = Array.Empty<OrchestrationState>();
            var lockReleaseRequests = new HashSet<(string EntityId, string LockOwner)>();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            do
            {
                SqlOrchestrationQuery entityInstancesQuery = new SqlOrchestrationQuery()
                {
                    PageSize = 100,
                    PageNumber = pageNumber,
                    InstanceIdPrefix = "@",
                    CreatedTimeFrom = DateTime.MinValue,
                    CreatedTimeTo = DateTime.MaxValue,
                    FetchInput = true,
                };
                IReadOnlyCollection<OrchestrationState> page = await this.orchestrationService.GetManyOrchestrationsAsync(entityInstancesQuery, cancellation);

                retrievedResults = page.Count;
                if (retrievedResults == 0)
                {
                    pageNumber = -1;
                    break;
                }

                var tasks = new List<Task>();
                var emptyEntityIds = new List<string>();

                foreach (OrchestrationState state in page)
                {
                    EntityStatus? status = ClientEntityHelpers.GetEntityStatus(state.Status);
                    if (status != null)
                    {
                        string? lockOwner = status.LockedBy;
                        if (request.ReleaseOrphanedLocks &&
                            lockOwner != null &&
                            lockReleaseRequests.Add((state.OrchestrationInstance.InstanceId, lockOwner)))
                        {
                            tasks.Add(CheckForOrphanedLockAndFixIt(state, lockOwner));
                        }

                        if (request.RemoveEmptyEntities)
                        {
                            bool isEmptyEntity = !status.EntityExists && status.LockedBy == null && status.BacklogQueueSize == 0;
                            bool safeToRemoveWithoutBreakingMessageSorterLogic =
                                now - state.LastUpdatedTime > this.orchestrationService.EntityBackendProperties.EntityMessageReorderWindow;
                            if (isEmptyEntity && safeToRemoveWithoutBreakingMessageSorterLogic)
                            {
                                emptyEntityIds.Add(state.OrchestrationInstance.InstanceId);
                            }
                        }
                    }
                }

                async Task CheckForOrphanedLockAndFixIt(OrchestrationState state, string lockOwner)
                {
                    OrchestrationState? ownerState
                        = (await this.orchestrationService.GetOrchestrationStateAsync(lockOwner, allExecutions: false)).FirstOrDefault();

                    bool OrchestrationIsRunning(OrchestrationStatus? status)
                        => status != null && (status == OrchestrationStatus.Running || status == OrchestrationStatus.Suspended);

                    if (!OrchestrationIsRunning(ownerState?.OrchestrationStatus))
                    {
                        // the owner is not a running orchestration. Send a lock release.
                        EntityMessageEvent eventToSend = ClientEntityHelpers.EmitUnlockForOrphanedLock(state.OrchestrationInstance, lockOwner);
                        await this.orchestrationService.SendTaskOrchestrationMessageAsync(eventToSend.AsTaskMessage());
                        Interlocked.Increment(ref orphanedLocksReleased);
                    }
                }

                await Task.WhenAll(tasks);
                emptyEntitiesRemoved += await this.orchestrationService.PurgeOrchestrationHistoryAsync(emptyEntityIds);
                if (emptyEntityIds.Count == 0)
                {
                    pageNumber++;
                }

            } while (retrievedResults > 0 && stopwatch.Elapsed <= TimeLimitForCleanEntityStorageLoop);

            return new CleanEntityStorageResult()
            {
                EmptyEntitiesRemoved = emptyEntitiesRemoved,
                OrphanedLocksReleased = orphanedLocksReleased,
                ContinuationToken = pageNumber < 0 ? null : pageNumber.ToString()
            };
        }

        EntityMetadata? GetEntityMetadata(OrchestrationState? state, bool includeTransient, bool includeState)
        {
            if (state == null)
            {
                return null;
            }

            if (!includeTransient && !EntityStatus.TestEntityExists(state.Status))
            {
                return null;
            }

            EntityStatus? status = ClientEntityHelpers.GetEntityStatus(state.Status);
            return new EntityMetadata()
            {
                EntityId = EntityId.FromString(state.OrchestrationInstance.InstanceId),
                LastModifiedTime = state.LastUpdatedTime,
                BacklogQueueSize = status?.BacklogQueueSize ?? 0,
                LockedBy = status?.LockedBy,
                SerializedState = includeState ? ExtractSerializedEntityState(state.Input) : null,
            };
        }

        static string? ExtractSerializedEntityState(string? entityMetadata)
        {
            if (entityMetadata is null || entityMetadata.Length == 0)
            {
                return null;
            }

            JObject entityJson = JObject.Parse(entityMetadata);
            if (entityJson.TryGetValue("exists", out JToken? existsValue) &&
                existsValue.Type == JTokenType.Boolean &&
                existsValue.Value<bool>() == false)
            {
                return null;
            }

            return entityJson.TryGetValue("state", out JToken? stateValue) ? stateValue.ToString(Newtonsoft.Json.Formatting.None) : null;
        }
    }
}
