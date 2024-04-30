// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;


    /// <summary>
    /// Common data and functions to help encapsulate how how orchestrations process messages
    /// </summary>
    public class OrchestrationCreation
    {
        public static Result HandleCreationRequest(OrchestrationState? existingState, TaskMessage taskMessage, OrchestrationStatus[]? dedupeStatuses)
        {
            OrchestrationStatus? existingStatus = existingState?.OrchestrationStatus;

            if (existingStatus.HasValue
                && dedupeStatuses != null
                && dedupeStatuses.Contains(existingStatus.Value))
            {
                return new Result.Deduplicate(existingStatus!.Value);
            }

            var ee = (ExecutionStartedEvent)taskMessage.Event;

            var newState = new OrchestrationState
            {
                Name = ee.Name,
                Version = ee.Version,
                OrchestrationInstance = ee.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                ParentInstance = ee.ParentInstance,
                Input = ee.Input,
                Tags = ee.Tags,
                CreatedTime = ee.Timestamp,
                LastUpdatedTime = ee.Timestamp,
                CompletedTime = DurableTask.Core.Common.DateTimeUtils.MinDateTime,
                ScheduledStartTime = ee.ScheduledStartTime,
                Status = null,
            };

            return new Result.NewOrReplace(newState);
        }

        public record Result
        {
            public record Deduplicate(OrchestrationStatus ExistingStatus) : Result;

            public record NewOrReplace(OrchestrationState State) : Result;
        }
    }
}
