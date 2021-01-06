namespace DurableTask.SqlServer.Logging
{
    using System;
    using DurableTask.Core;
    using DurableTask.Core.History;
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
    }
}
