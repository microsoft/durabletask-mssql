// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Microsoft.Data.SqlClient;

    class SqlOrchestrationSession : IOrchestrationSession
    {
        internal const int LockLostErrorNumber = 50003;

        readonly SqlOrchestrationServiceSettings settings;
        readonly BackoffPollingHelper orchestrationBackoffHelper;
        readonly LogHelper traceHelper;
        readonly EventPayloadMap eventPayloadMappings;
        readonly string instanceId;
        readonly string lockedByValue;
        readonly CancellationToken shutdownToken;

        public SqlOrchestrationSession(
            SqlOrchestrationServiceSettings settings,
            BackoffPollingHelper orchestrationBackoffHelper,
            LogHelper traceHelper,
            EventPayloadMap eventPayloadMappings,
            string instanceId,
            string lockedByValue,
            CancellationToken shutdownToken)
        {
            this.settings = settings;
            this.orchestrationBackoffHelper = orchestrationBackoffHelper;
            this.traceHelper = traceHelper;
            this.eventPayloadMappings = eventPayloadMappings;
            this.instanceId = instanceId;
            this.lockedByValue = lockedByValue;
            this.shutdownToken = shutdownToken;
        }

        public async Task<IList<TaskMessage>?> FetchNewOrchestrationMessagesAsync(TaskOrchestrationWorkItem workItem)
        {
            DateTime deadline = DateTime.UtcNow + this.settings.ExtendedSessionIdleTimeout;
            while (!this.shutdownToken.IsCancellationRequested)
            {
                DateTime newLockExpiration = DateTime.UtcNow + this.settings.WorkItemLockTimeout;
                IList<TaskMessage> messages;
                try
                {
                    messages = await this.FetchAsync(newLockExpiration);
                }
                catch (SqlException e) when (HasErrorNumber(e, LockLostErrorNumber))
                {
                    throw new SessionAbortedException(
                        $"Lost the lock for instance '{this.instanceId}'.", e);
                }
                catch (OperationCanceledException) when (this.shutdownToken.IsCancellationRequested)
                {
                    return null;
                }

                if (messages.Count > 0)
                {
                    workItem.LockedUntilUtc = newLockExpiration;
                    return messages;
                }

                if (DateTime.UtcNow >= deadline)
                {
                    return null;
                }

                try
                {
                    await this.orchestrationBackoffHelper.WaitAsync(this.shutdownToken);
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
            }

            return null;
        }

        public async Task ReleaseLockAsync()
        {
            try
            {
                using SqlConnection connection = this.settings.CreateConnection();
                await connection.OpenAsync();
                using SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = $"{this.settings.SchemaName}._ReleaseOrchestrationLock";
                command.Parameters.Add("@InstanceID", SqlDbType.VarChar, 100).Value = this.instanceId;
                command.Parameters.Add("@LockedBy", SqlDbType.VarChar, 100).Value = this.lockedByValue;

                await SqlUtils.ExecuteNonQueryAsync(command, this.traceHelper, this.instanceId);
            }
            catch (Exception e)
            {
                // Best-effort release; the lock will expire naturally after WorkItemLockTimeout if this fails.
                this.traceHelper.GenericWarning(
                    $"Failed to release orchestration lock for instance '{this.instanceId}': {e.Message}",
                    this.instanceId);
            }
        }

        async Task<IList<TaskMessage>> FetchAsync(DateTime newLockExpiration)
        {
            using SqlConnection connection = this.settings.CreateConnection();
            await connection.OpenAsync(this.shutdownToken);

            using SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = $"{this.settings.SchemaName}._FetchOrchestrationMessages";
            command.Parameters.Add("@InstanceID", SqlDbType.VarChar, 100).Value = this.instanceId;
            command.Parameters.Add("@LockedBy", SqlDbType.VarChar, 100).Value = this.lockedByValue;
            command.Parameters.Add("@LockExpiration", SqlDbType.DateTime2).Value = newLockExpiration;
            command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = this.settings.WorkItemBatchSize;

            using DbDataReader reader = await SqlUtils.ExecuteReaderAsync(
                command,
                this.traceHelper,
                this.instanceId,
                this.shutdownToken);

            var messages = new List<TaskMessage>(capacity: this.settings.WorkItemBatchSize);
            while (reader.Read())
            {
                TaskMessage message = reader.GetTaskMessage();
                messages.Add(message);
                Guid? payloadId = reader.GetPayloadId();
                if (payloadId.HasValue)
                {
                    this.eventPayloadMappings.Add(message.Event, payloadId.Value);
                }
            }

            return messages;
        }

        static bool HasErrorNumber(SqlException ex, int errorNumber)
        {
            foreach (SqlError error in ex.Errors)
            {
                if (error.Number == errorNumber)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
