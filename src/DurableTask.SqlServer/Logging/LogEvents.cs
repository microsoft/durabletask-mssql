// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Logging
{
    using System;
    using DurableTask.Core;
    using DurableTask.Core.Logging;
    using Microsoft.Extensions.Logging;

    class LogEvents
    {
        internal class AcquiredAppLockEvent : StructuredLogEvent, IEventSourceEvent
        {
            public AcquiredAppLockEvent(int statusCode, long latencyMs)
            {
                this.StatusCode = statusCode;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public int StatusCode { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override LogLevel Level => LogLevel.Information;

            public override EventId EventId => new EventId(
                EventIds.AcquiredAppLock,
                nameof(EventIds.AcquiredAppLock));

            /// <inheritdoc />
            protected override string CreateLogMessage() =>
                $"Acquired app lock after {this.LatencyMs}ms (status code = {this.StatusCode})";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.AcquiredAppLock(
                    this.StatusCode,
                    this.LatencyMs,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class ExecutedSqlScriptEvent : StructuredLogEvent, IEventSourceEvent
        {
            public ExecutedSqlScriptEvent(string name, long latencyMs)
            {
                this.Name = name;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.ExecutedSqlScript,
                nameof(EventIds.ExecutedSqlScript));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Executed SQL script {this.Name} in {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.ExecutedSqlScript(
                    this.Name,
                    this.LatencyMs,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class SprocCompletedEvent : StructuredLogEvent, IEventSourceEvent
        {
            public SprocCompletedEvent(string name, long latencyMs, int retryCount, string? instanceId)
            {
                this.Name = name;
                this.LatencyMs = latencyMs;
                this.RetryCount = retryCount;
                this.InstanceId = instanceId;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public int RetryCount { get; }

            [StructuredLogField]
            public string? InstanceId { get; }

            public override LogLevel Level => LogLevel.Debug;

            public override EventId EventId => new EventId(
                EventIds.SprocCompleted,
                nameof(EventIds.SprocCompleted));

            protected override string CreateLogMessage() =>
                string.IsNullOrEmpty(this.InstanceId) ?
                    $"Executed stored procedure {this.Name} in {this.LatencyMs}ms" :
                    $"{this.InstanceId}: Executed stored procedure {this.Name} in {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.SprocCompleted(
                    this.InstanceId,
                    this.Name,
                    this.LatencyMs,
                    this.RetryCount,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class ProcessingErrorEvent : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessingErrorEvent(Exception e, OrchestrationInstance instance)
            {
                this.Details = e.ToString();
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
            }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            public override EventId EventId => new EventId(
                EventIds.ProcessingFailure,
                nameof(EventIds.ProcessingFailure));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: An error occurred while processing a work-item: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.ProcessingError(
                    this.Details,
                    this.InstanceId,
                    this.ExecutionId,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class GenericWarning : StructuredLogEvent, IEventSourceEvent
        {
            public GenericWarning(
                string details,
                string? instanceId)
            {
                this.Details = details;
                this.InstanceId = instanceId;
            }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public string? InstanceId { get; }

            public override EventId EventId => new EventId(
                EventIds.GenericWarning,
                nameof(EventIds.GenericWarning));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.IsNullOrEmpty(this.InstanceId) ?
                this.Details :
                $"{this.InstanceId}: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.GenericWarning(
                    this.Details,
                    this.InstanceId ?? string.Empty,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class CheckpointStartingEvent : StructuredLogEvent, IEventSourceEvent
        {
            public CheckpointStartingEvent(
                string name,
                OrchestrationInstance instance,
                OrchestrationStatus status)
            {
                this.Name = name;
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.Status = status.ToString();
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Status { get; }

            public override EventId EventId => new EventId(
                EventIds.CheckpointStarting,
                nameof(EventIds.CheckpointStarting));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Checkpointing orchestration '{this.Name}' with status: {this.Status}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.CheckpointStarting(
                    this.Name,
                    this.InstanceId,
                    this.ExecutionId,
                    this.Status,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class CheckpointCompletedEvent : StructuredLogEvent, IEventSourceEvent
        {
            public CheckpointCompletedEvent(
                string name,
                OrchestrationInstance instance,
                long latencyMs)
            {
                this.Name = name;
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.CheckpointCompleted,
                nameof(EventIds.CheckpointCompleted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Checkpoint of '{this.Name}' completed. Latency: {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.CheckpointCompleted(
                    this.Name,
                    this.InstanceId,
                    this.ExecutionId,
                    this.LatencyMs,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class DuplicateExecutionDetected : StructuredLogEvent, IEventSourceEvent
        {
            public DuplicateExecutionDetected(
                string instanceId,
                string executionId,
                string name)
            {
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.Name = name;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            public override EventId EventId => new EventId(
                EventIds.DuplicateExecutionDetected,
                nameof(EventIds.DuplicateExecutionDetected));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Duplicate execution of '{this.Name}' was detected! This means two workers or threads tried to process the same work item simultaneously. The result of this execution will be discarded.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.DuplicateExecutionDetected(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class TransientDatabaseFailure : StructuredLogEvent, IEventSourceEvent
        {
            public TransientDatabaseFailure(Exception e, string? instanceId, int retryCount)
            {
                this.InstanceId = instanceId;
                this.RetryCount = retryCount;
                this.Details = e.ToString();
            }

            [StructuredLogField]
            public string? InstanceId { get; }

            [StructuredLogField]
            public int RetryCount { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.TransientDatabaseFailure,
                nameof(EventIds.TransientDatabaseFailure));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId ?? "Non-orchestration failure"}: A transient database failure occurred and will be retried. Current retry count: {this.RetryCount}. Details: {this.Details}.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.TransientDatabaseFailure(
                    this.InstanceId,
                    this.RetryCount,
                    this.Details,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class ReplicaCountChangeRecommended : StructuredLogEvent, IEventSourceEvent
        {
            public ReplicaCountChangeRecommended(int currentCount, int recommendedCount)
            {
                this.CurrentCount = currentCount;
                this.RecommendedCount = recommendedCount;
            }

            [StructuredLogField]
            public int CurrentCount { get; }

            [StructuredLogField]
            public int RecommendedCount { get; }

            public override EventId EventId => new EventId(
                EventIds.ReplicaCountChangeRecommended,
                nameof(EventIds.ReplicaCountChangeRecommended));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Recommending a replica count change from {this.CurrentCount} to {this.RecommendedCount}.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.ReplicaCountChangeRecommended(
                    this.CurrentCount,
                    this.RecommendedCount,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class PurgedInstances : StructuredLogEvent, IEventSourceEvent
        {
            public PurgedInstances(string userId, int purgedInstanceCount, long latencyMs)
            {
                this.UserId = userId;
                this.InstanceCount = purgedInstanceCount;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public int InstanceCount { get; }

            [StructuredLogField]
            public string UserId { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.PurgedInstances,
                nameof(EventIds.PurgedInstances));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"User '{this.UserId}' purged {this.InstanceCount} orchestration instances. Latency = {this.LatencyMs}ms.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.PurgedInstances(
                    this.UserId,
                    this.InstanceCount,
                    this.LatencyMs,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class CommandCompletedEvent : StructuredLogEvent, IEventSourceEvent
        {
            public CommandCompletedEvent(string commandText, long latencyMs, int retryCount, string? instanceId)
            {
                this.CommandText = commandText;
                this.LatencyMs = latencyMs;
                this.RetryCount = retryCount;
                this.InstanceId = instanceId;
            }

            [StructuredLogField]
            public string CommandText { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public int RetryCount { get; }

            [StructuredLogField]
            public string? InstanceId { get; }

            public override LogLevel Level => LogLevel.Debug;

            public override EventId EventId => new EventId(
                EventIds.CommandCompleted,
                nameof(EventIds.CommandCompleted));

            protected override string CreateLogMessage() =>
                string.IsNullOrEmpty(this.InstanceId) ?
                    $"Executed SQL statement(s) '{this.CommandText}' in {this.LatencyMs}ms" :
                    $"{this.InstanceId}: Executed SQL statement(s) '{this.CommandText}' in {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.CommandCompleted(
                    this.InstanceId,
                    this.CommandText,
                    this.LatencyMs,
                    this.RetryCount,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class CreatedDatabaseEvent : StructuredLogEvent, IEventSourceEvent
        {
            public CreatedDatabaseEvent(string databaseName) =>
                this.DatabaseName = databaseName;

            [StructuredLogField]
            public string DatabaseName { get; }

            public override EventId EventId => new EventId(
                EventIds.CreatedDatabase,
                nameof(EventIds.CreatedDatabase));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"Created database '{this.DatabaseName}'.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.CreatedDatabase(
                    this.DatabaseName,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class DiscardingEventEvent : StructuredLogEvent, IEventSourceEvent
        {
            public DiscardingEventEvent(string instanceId, string eventType, int taskEventId, string details)
            {
                this.InstanceId = instanceId;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.Details = details;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.DiscardingEvent,
                nameof(EventIds.DiscardingEvent));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Discarding {GetEventDescription(this.EventType, this.TaskEventId)}: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.DiscardingEvent(
                    this.InstanceId,
                    this.EventType,
                    this.TaskEventId,
                    this.Details,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class GenericInfo : StructuredLogEvent, IEventSourceEvent
        {
            public GenericInfo(string details, string? instanceId)
            {
                this.Details = details;
                this.InstanceId = instanceId;
            }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public string? InstanceId { get; }

            public override EventId EventId => new EventId(
                EventIds.GenericInfo,
                nameof(EventIds.GenericInfo));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => string.IsNullOrEmpty(this.InstanceId) ?
                this.Details :
                $"{this.InstanceId}: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.GenericInfo(
                    this.Details,
                    this.InstanceId ?? string.Empty,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }
    }
}
