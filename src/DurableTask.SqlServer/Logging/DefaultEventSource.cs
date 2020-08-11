namespace DurableTask.SqlServer.Logging
{
    using System;
    using System.Diagnostics.Tracing;

    /// <summary>
    /// Event Source for DurableTask-SqlServer.
    /// </summary>
    /// <remarks>
    /// The names of the fields are intended to match those in DurableTask-Core, DurableTask-AzureStorage,
    /// and WebJobs-Extensions-DurableTask whenever possible.
    /// The provider GUID value is 4BA38912-E64F-5FD2-170D-68AC65B1E58D.
    /// </remarks>
    [EventSource(Name = "DurableTask-SqlServer")]
    class DefaultEventSource : EventSource
    {
        public static readonly DefaultEventSource Log = new DefaultEventSource();

        [Event(EventIds.ExecutedSqlScript, Level = EventLevel.Informational)]
        public void ExecutedSqlScript(
            string Name,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.ExecutedSqlScript,
                Name,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.SprocCompleted, Level = EventLevel.Verbose)]
        public void SprocCompleted(
            string Name,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.SprocCompleted,
                Name,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.AcquiredAppLock, Level = EventLevel.Informational)]
        public void AcquiredAppLock(
            int StatusCode,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.AcquiredAppLockCore(
                EventIds.AcquiredAppLock,
                StatusCode,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }

        [NonEvent]
        unsafe void AcquiredAppLockCore(int eventId, int statusCode, long latencyMs, string appName, string extensionVersion)
        {
            // This needs to be done manually because the built-in WriteEvent(int, long, long) overload will overwrite data (as shown in testing)
            fixed (char* appNamePtr = appName)
            fixed (char* extensionVersionPtr = extensionVersion)
            {
                EventData* data = stackalloc EventData[4];
                data[0].DataPointer = (IntPtr)(&statusCode);
                data[0].Size = sizeof(int);
                data[1].DataPointer = (IntPtr)(&latencyMs);
                data[1].Size = sizeof(long);
                data[2].DataPointer = (IntPtr)appNamePtr;
                data[2].Size = (appName.Length + 1) + 2;
                data[3].DataPointer = (IntPtr)extensionVersionPtr;
                data[3].Size = (extensionVersion.Length + 1) + 2;

                this.WriteEventCore(eventId, 2, data);
            }
        }

        [Event(EventIds.CheckpointingOrchestration, Level = EventLevel.Informational)]
        public void CheckpointingOrchestration(
            string Name,
            string InstanceId,
            string ExecutionId,
            string Status,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Switch to WriteEventCore for better performance
            this.WriteEvent(
                EventIds.CheckpointingOrchestration,
                Name,
                InstanceId,
                ExecutionId,
                Status,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.ProcessingFailure, Level = EventLevel.Error)]
        public void ProcessingError(
            string Details,
            string InstanceId,
            string ExecutionId,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.ProcessingFailure,
                Details,
                InstanceId,
                ExecutionId ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.SchedulingLocalActivity, Level = EventLevel.Informational)]
        public void SchedulingLocalActivity(
            string Name,
            int TaskId,
            string InstanceId,
            string ExecutionId,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.SchedulingLocalActivity,
                Name,
                TaskId,
                InstanceId,
                ExecutionId,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.StartingLocalActivity, Level = EventLevel.Informational)]
        public void StartingLocalActivity(
            string Name,
            int TaskId,
            string InstanceId,
            string ExecutionId,
            int WaitTimeMs, string AppName, string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.StartingLocalActivity,
                Name,
                TaskId,
                InstanceId,
                ExecutionId,
                WaitTimeMs,
                AppName,
                ExtensionVersion);
        }
    }
}
