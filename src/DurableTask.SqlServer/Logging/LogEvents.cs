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
            public SprocCompletedEvent(string name, long latencyMs)
            {
                this.Name = name;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override LogLevel Level => LogLevel.Debug;

            public override EventId EventId => new EventId(
                EventIds.SprocCompleted,
                nameof(EventIds.SprocCompleted));

            protected override string CreateLogMessage() =>
                $"Executed stored procedure {this.Name} in {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.SprocCompleted(
                    this.Name,
                    this.LatencyMs,
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
                $"An error occurred while processing a work-item for instance '{this.InstanceId}': {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.ProcessingError(
                    this.Details,
                    this.InstanceId,
                    this.ExecutionId,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class SchedulingLocalActivityEvent : StructuredLogEvent, IEventSourceEvent
        {
            public SchedulingLocalActivityEvent(TaskScheduledEvent e, OrchestrationInstance instance)
            {
                this.Name = e.Name;
                this.TaskEventId = e.EventId;
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            public override EventId EventId => new EventId(
                EventIds.SchedulingLocalActivity,
                nameof(EventIds.SchedulingLocalActivity));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Scheduling activity '{this.Name}' (TaskID = {this.TaskEventId}) for local execution.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.SchedulingLocalActivity(
                    this.Name,
                    this.TaskEventId,
                    this.InstanceId,
                    this.ExecutionId,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class StartingLocalActivityEvent : StructuredLogEvent, IEventSourceEvent
        {
            public StartingLocalActivityEvent(TaskScheduledEvent e, OrchestrationInstance instance, int waitTimeMs)
            {
                this.Name = e.Name;
                this.TaskEventId = e.EventId;
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.WaitTimeMs = waitTimeMs;
            }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int WaitTimeMs { get; }

            public override EventId EventId => new EventId(
                EventIds.StartingLocalActivity,
                nameof(EventIds.StartingLocalActivity));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Activity '{this.Name}' (TaskID = {this.TaskEventId}) started executing.";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.StartingLocalActivity(
                    this.Name,
                    this.TaskEventId,
                    this.InstanceId,
                    this.ExecutionId,
                    this.WaitTimeMs,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }

        internal class CheckpointingOrchestrationEvent : StructuredLogEvent, IEventSourceEvent
        {
            public CheckpointingOrchestrationEvent(
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
                EventIds.CheckpointingOrchestration,
                nameof(EventIds.CheckpointingOrchestration));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Checkpointing orchestration '{this.Name}' with ID {this.InstanceId} and status: {this.Status}";

            void IEventSourceEvent.WriteEventSource() =>
                DefaultEventSource.Log.CheckpointingOrchestration(
                    this.Name,
                    this.InstanceId,
                    this.ExecutionId,
                    this.Status,
                    DTUtils.AppName,
                    DTUtils.ExtensionVersionString);
        }
    }
}
