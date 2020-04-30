namespace DurableTask.SqlServer
{
    using System;
    using System.Diagnostics;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;

    class TraceHelper
    {
        readonly ILogger log;

        public TraceHelper(ILogger log)
        {
            this.log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public void ExecutedSqlScript(string name, Stopwatch latencyStopwatch)
            => Log.ExecutedSqlScript(this.log, name, latencyStopwatch);

        public void SprocCompleted(string name, Stopwatch latencyStopwatch)
            => Log.SprocCompleted(this.log, name, latencyStopwatch);

        public void AcquiredAppLock(int statusCode, Stopwatch latencyStopwatch)
            => Log.AcquiredAppLock(this.log, statusCode, latencyStopwatch);

        public void SchedulingOrchestration(ExecutionStartedEvent @event)
            => Log.SchedulingOrchestration(this.log, @event.Name, @event.OrchestrationInstance);

        public void ResumingOrchestration(string name, OrchestrationInstance instance, int waitTimeMs)
            => Log.ResumingOrchestration(this.log, name, instance, waitTimeMs);

        public void CheckpointingOrchestration(OrchestrationState state)
            => Log.CheckpointingOrchestration(this.log, state.Name, state.OrchestrationInstance, state.OrchestrationStatus);

        public void ProcessingError(Exception e, OrchestrationInstance instance)
            => Log.ProcessingError(this.log, e, instance);

        public void SchedulingActivity(TaskScheduledEvent @event, OrchestrationInstance instance, bool isLocal)
            => Log.SchedulingActivity(this.log, @event, instance, isLocal);

        public void StartingActivity(TaskScheduledEvent @event, OrchestrationInstance instance, int waitTimeMs)
            => Log.StartingActivity(this.log, @event, instance, waitTimeMs);

        public void CompletingActivity(OrchestrationInstance instance, int taskId, bool succeeded)
            => Log.CompletingActivity(this.log, instance, taskId, succeeded);

        static class Log
        {
            static readonly Action<ILogger, string, long, Exception?> LogExecutedSqlScript =
                LoggerMessage.Define<string, long>(
                    LogLevel.Information,
                    Events.ExecutedSqlScript,
                    "Executed SQL script: {Name}, LatencyMs: {LatencyMs}"); 
            
            static readonly Action<ILogger, string, long, Exception?> LogSprocCompleted =
                LoggerMessage.Define<string, long>(
                    LogLevel.Trace,
                    Events.SprocCompleted,
                    "Sproc completed. Name: {Name}, LatencyMs: {LatencyMs}");
            
            static readonly Action<ILogger, int, long, Exception?> LogAcquiredAppLock =
                LoggerMessage.Define<int, long>(
                    LogLevel.Information,
                    Events.AcquiredAppLock,
                    "Acquired app lock. Status code: {StatusCode}, LatencyMs: {LatencyMs}");

            static readonly Action<ILogger, string, string, string, Exception?> LogSchedulingOrchestration =
                LoggerMessage.Define<string, string, string>(
                    LogLevel.Information,
                    Events.SchedulingOrchestration,
                    "Scheduling orchestration. Name: {Name}, InstanceId: {InstanceId}, ExecutionId: {ExecutionId}");

            static readonly Action<ILogger, string, string, string, int, Exception?> LogResumingOrchestration =
                LoggerMessage.Define<string, string, string, int>(
                    LogLevel.Information,
                    Events.StartingOrchestration,
                    "Starting or resuming orchestration. Name: {Name}, InstanceId: {InstanceId}, ExecutionId: {ExecutionId}, WaitTime: {WaitTime}");

            static readonly Action<ILogger, string, string, string, OrchestrationStatus, Exception?> LogCheckpointingOrchestration =
                LoggerMessage.Define<string, string, string, OrchestrationStatus>(
                    LogLevel.Information,
                    Events.CheckpointingOrchestration,
                    "Checkpointing orchestration. Name: {Name}, InstanceId: {InstanceId}, ExecutionId: {ExecutionId}, Status: {Status}");

            static readonly Action<ILogger, string, string, Exception?> LogProcessingError =
                LoggerMessage.Define<string, string>(
                    LogLevel.Error,
                    Events.ProcessingFailure,
                    "Processing error. InstanceId: {InstanceId}, ExecutionId: {ExecutionId}");

            static readonly Action<ILogger, string, string, string, int, bool, Exception?> LogSchedulingActivity =
                LoggerMessage.Define<string, string, string, int, bool>(
                    LogLevel.Information,
                    Events.SchedulingActivity,
                    "Scheduling activity. Name: {Name}, InstanceId: {InstanceId}, ExecutionId: {ExecutionId}, TaskId: {TaskId}, IsLocal: {IsLocal}");

            static readonly Action<ILogger, string, string, string, int, int, Exception?> LogStartingActivity =
                LoggerMessage.Define<string, string, string, int, int>(
                    LogLevel.Information,
                    Events.StartingActivity,
                    "Starting activity. Name: {Name}, InstanceId: {InstanceId}, ExecutionId: {ExecutionId}, TaskId: {TaskId}, WaitTime: {WaitTime}");

            static readonly Action<ILogger, string, string, int, bool, Exception?> LogCompletingActivity =
                LoggerMessage.Define<string, string, int, bool>(
                    LogLevel.Information,
                    Events.CompletingActivity,
                    "Completing activity. InstanceId: {InstanceId}, ExecutionId: {ExecutionId}, TaskId: {TaskId}, Succeeded: {Succeeded}");

            internal static void AcquiredAppLock(ILogger log, int statusCode, Stopwatch stopwatch)
                => LogAcquiredAppLock(log, statusCode, stopwatch.ElapsedMilliseconds, null);

            internal static void ExecutedSqlScript(ILogger log, string name, Stopwatch latencyStopwatch)
                => LogExecutedSqlScript(log, name, latencyStopwatch.ElapsedMilliseconds, null);

            internal static void SprocCompleted(ILogger log, string name, Stopwatch stopwatch)
                => LogSprocCompleted(log, name, stopwatch.ElapsedMilliseconds, null);

            internal static void SchedulingOrchestration(ILogger log, string name, OrchestrationInstance instance)
                => LogSchedulingOrchestration(log, name, instance.InstanceId, instance.ExecutionId, null);

            internal static void ResumingOrchestration(ILogger log, string name, OrchestrationInstance instance, int waitTimeMs)
                => LogResumingOrchestration(log, name, instance.InstanceId, instance.ExecutionId, waitTimeMs, null);

            internal static void CheckpointingOrchestration(ILogger log, string name, OrchestrationInstance instance, OrchestrationStatus status)
                => LogCheckpointingOrchestration(log, name, instance.InstanceId, instance.ExecutionId, status, null);

            internal static void ProcessingError(ILogger log, Exception e, OrchestrationInstance instance)
                => LogProcessingError(log, instance.InstanceId, instance.ExecutionId, e);

            internal static void SchedulingActivity(ILogger log, TaskScheduledEvent @event, OrchestrationInstance instance, bool isLocal)
                => LogSchedulingActivity(log, @event.Name, instance.InstanceId, instance.ExecutionId, @event.EventId, isLocal, null);

            internal static void StartingActivity(ILogger log, TaskScheduledEvent @event, OrchestrationInstance instance, int waitTimeMs)
                => LogStartingActivity(log, @event.Name, instance.InstanceId, instance.ExecutionId, @event.EventId, waitTimeMs, null);

            internal static void CompletingActivity(ILogger log, OrchestrationInstance instance, int taskId, bool succeeded)
                => LogCompletingActivity(log, instance.InstanceId, instance.ExecutionId, taskId, succeeded, null);
        }

        static class Events
        {
            // Database operations
            public static readonly EventId AcquiredAppLock = new EventId(300, nameof(AcquiredAppLock));
            public static readonly EventId ExecutedSqlScript = new EventId(301, nameof(ExecutedSqlScript));
            public static readonly EventId SprocCompleted = new EventId(302, nameof(SprocCompleted));

            // Orchestration operations
            public static readonly EventId SchedulingOrchestration = new EventId(310, nameof(SchedulingOrchestration));
            public static readonly EventId StartingOrchestration = new EventId(311, nameof(StartingOrchestration));
            public static readonly EventId CheckpointingOrchestration = new EventId(312, nameof(CheckpointingOrchestration));
            public static readonly EventId AbortingOrchestration = new EventId(313, nameof(AbortingOrchestration));
            public static readonly EventId RenewingOrchestration = new EventId(314, nameof(RenewingOrchestration));
            public static readonly EventId ProcessingFailure = new EventId(315, nameof(ProcessingFailure));

            // Activity operations
            public static readonly EventId SchedulingActivity = new EventId(320, nameof(SchedulingActivity));
            public static readonly EventId StartingActivity = new EventId(321, nameof(StartingActivity));
            public static readonly EventId CompletingActivity = new EventId(322, nameof(CompletingActivity));
            public static readonly EventId AbortingActivity = new EventId(323, nameof(AbortingActivity));
            public static readonly EventId RenewingActivity = new EventId(324, nameof(RenewingActivity));
        }
    }
}
