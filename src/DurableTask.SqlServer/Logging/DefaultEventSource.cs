﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
            string? InstanceId,
            string Name,
            long LatencyMs,
            int RetryCount,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Switch to WriteEventCore for better performance
            this.WriteEvent(
                EventIds.SprocCompleted,
                InstanceId ?? string.Empty,
                Name,
                LatencyMs,
                RetryCount,
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

        [Event(EventIds.CheckpointStarting, Level = EventLevel.Informational)]
        public void CheckpointStarting(
            string Name,
            string InstanceId,
            string ExecutionId,
            string Status,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Switch to WriteEventCore for better performance
            this.WriteEvent(
                EventIds.CheckpointStarting,
                Name,
                InstanceId,
                ExecutionId,
                Status,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.CheckpointCompleted, Level = EventLevel.Informational)]
        public void CheckpointCompleted(
            string Name,
            string InstanceId,
            string ExecutionId,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Switch to WriteEventCore for better performance
            this.WriteEvent(
                EventIds.CheckpointCompleted,
                Name,
                InstanceId,
                ExecutionId,
                LatencyMs,
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

        [Event(EventIds.GenericWarning, Level = EventLevel.Warning)]
        internal void GenericWarning(
            string Details,
            string InstanceId,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.GenericWarning,
                Details,
                InstanceId,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.DuplicateExecutionDetected, Level = EventLevel.Warning)]
        internal void DuplicateExecutionDetected(
            string InstanceId,
            string ExecutionId,
            string Name,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.DuplicateExecutionDetected,
                InstanceId,
                ExecutionId ?? string.Empty,
                Name,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.TransientDatabaseFailure, Level = EventLevel.Warning)]
        internal void TransientDatabaseFailure(
            string? InstanceId,
            int RetryCount,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.TransientDatabaseFailure,
                InstanceId ?? string.Empty,
                RetryCount,
                Details,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.ReplicaCountChangeRecommended, Level = EventLevel.Informational)]
        internal void ReplicaCountChangeRecommended(
            int CurrentCount,
            int RecommendedCount,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.ReplicaCountChangeRecommended,
                CurrentCount,
                RecommendedCount,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PurgedInstances, Level = EventLevel.Informational)]
        internal void PurgedInstances(
            string UserId,
            int InstanceCount,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            // TODO: Use WriteEventCore for better performance
            this.WriteEvent(
                EventIds.PurgedInstances,
                UserId,
                InstanceCount,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }
    }
}
