namespace DurableTask.SqlServer
{
    using System;
    using System.Diagnostics;
    using DurableTask.Core;
    using DurableTask.Core.Logging;
    using DurableTask.SqlServer.Logging;
    using Microsoft.Extensions.Logging;

    class LogHelper
    {
        readonly ILogger log;

        public LogHelper(ILogger log)
        {
            this.log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public void ExecutedSqlScript(string name, Stopwatch latencyStopwatch)
        {
            var logEvent = new LogEvents.ExecutedSqlScriptEvent(
                name,
                latencyStopwatch.ElapsedMilliseconds);

            this.WriteLog(logEvent);
        }

        public void SprocCompleted(string name, Stopwatch latencyStopwatch)
        {
            var logEvent = new LogEvents.SprocCompletedEvent(
                name,
                latencyStopwatch.ElapsedMilliseconds);

            this.WriteLog(logEvent);
        }

        public void AcquiredAppLock(int statusCode, Stopwatch latencyStopwatch)
        {
            var logEvent = new LogEvents.AcquiredAppLockEvent(
                statusCode,
                latencyStopwatch.ElapsedMilliseconds);
            
            this.WriteLog(logEvent);
        }

        public void ProcessingError(Exception e, OrchestrationInstance instance)
        {
            var logEvent = new LogEvents.ProcessingErrorEvent(e, instance);
            this.WriteLog(logEvent);
        }

        public void GenericWarning(string details, string? instanceId)
        {
            var logEvent = new LogEvents.GenericWarning(details, instanceId);
            this.WriteLog(logEvent);
        }

        public void CheckpointStarting(OrchestrationState state)
        {
            var logEvent = new LogEvents.CheckpointStartingEvent(
                state.Name,
                state.OrchestrationInstance,
                state.OrchestrationStatus);

            this.WriteLog(logEvent);
        }


        public void CheckpointCompleted(OrchestrationState state, Stopwatch latencyStopwatch)
        {
            var logEvent = new LogEvents.CheckpointCompletedEvent(
                state.Name,
                state.OrchestrationInstance,
                latencyStopwatch.ElapsedMilliseconds);

            this.WriteLog(logEvent);
        }

        public void DuplicateExecutionDetected(OrchestrationInstance instance, string name)
        {
            var logEvent = new LogEvents.DuplicateExecutionDetected(
                instance.InstanceId,
                instance.ExecutionId,
                name);
            this.WriteLog(logEvent);
        }

        void WriteLog(ILogEvent logEvent)
        {
            // LogDurableEvent is an extension method defined in DurableTask.Core
            this.log.LogDurableEvent(logEvent);
        }
    }
}
