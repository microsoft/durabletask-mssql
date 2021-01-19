// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.SqlTypes
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlTypes;
    using System.Linq;
    using DurableTask.Core;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;

    static class OrchestrationEventSqlType
    {
        const string SqlTypeName = "dt.OrchestrationEvents";

        static readonly SqlMetaData[] OrchestrationEventSchema = new SqlMetaData[]
        {
            // IMPORTANT: The order and schema of these items must always match the order of the SQL type in logic.sql
            new SqlMetaData("InstanceID", SqlDbType.VarChar, 100),
            new SqlMetaData("ExecutionID", SqlDbType.VarChar, 50),
            new SqlMetaData("SequenceNumber", SqlDbType.BigInt),
            new SqlMetaData("EventType", SqlDbType.VarChar, 30),
            new SqlMetaData("Name", SqlDbType.VarChar, 300),
            new SqlMetaData("RuntimeStatus", SqlDbType.VarChar, 30),
            new SqlMetaData("TaskID", SqlDbType.Int),
            new SqlMetaData("VisibleTime", SqlDbType.DateTime2),
            new SqlMetaData("Reason", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadText", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadID", SqlDbType.UniqueIdentifier),
        };

        static class ColumnOrdinals
        {
            // IMPORTANT: Must be kept in sync with the database schema
            public const int InstanceID = 0;
            public const int ExecutionID = 1;
            public const int SequenceNumber = 2;
            public const int EventType = 3;
            public const int Name = 4;
            public const int RuntimeStatus = 5;
            public const int TaskID = 6;
            public const int VisibleTime = 7;
            public const int Reason = 8;
            public const int PayloadText = 9;
            public const int PayloadId = 10;
        }

        public static SqlParameter AddOrchestrationEventsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;

            IEnumerable<TaskMessage> messages = orchestratorMessages.Union(timerMessages);
            if (continuedAsNewMessage != null)
            {
                messages = messages.Append(continuedAsNewMessage);
            }

            param.Value = ToOrchestrationMessageParameter(messages);
            return param;
        }

        public static SqlParameter AddOrchestrationEventsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            TaskMessage message)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;
            param.Value = ToOrchestrationMessageParameter(message);
            return param;
        }

        static IEnumerable<SqlDataRecord>? ToOrchestrationMessageParameter(
            this IEnumerable<TaskMessage> messages)
        {
            if (!messages.Any())
            {
                // ADO.NET requires a null value instead of an empty enumerator
                // when passing a table-value parameter with zero entries.
                return null;
            }

            return GetOrchestrationMessageRecords();

            // Using a local function to support using null and yield syntax in the same method
            IEnumerable<SqlDataRecord> GetOrchestrationMessageRecords()
            {
                var record = new SqlDataRecord(OrchestrationEventSchema);
                foreach (TaskMessage msg in messages)
                {
                    yield return PopulateOrchestrationMessage(msg, record);
                }
            }
        }

        static IEnumerable<SqlDataRecord> ToOrchestrationMessageParameter(TaskMessage msg)
        {
            var record = new SqlDataRecord(OrchestrationEventSchema);
            yield return PopulateOrchestrationMessage(msg, record);
        }

        static SqlDataRecord PopulateOrchestrationMessage(TaskMessage msg, SqlDataRecord record)
        {
            string instanceId = msg.OrchestrationInstance.InstanceId;

            record.SetSqlString(ColumnOrdinals.InstanceID, instanceId);
            record.SetSqlString(ColumnOrdinals.ExecutionID, msg.OrchestrationInstance.ExecutionId);
            record.SetSqlInt64(ColumnOrdinals.SequenceNumber, msg.SequenceNumber);
            record.SetSqlString(ColumnOrdinals.EventType, msg.Event.EventType.ToString());
            record.SetSqlString(ColumnOrdinals.Name, SqlUtils.GetName(msg.Event));
            record.SetSqlString(ColumnOrdinals.RuntimeStatus, SqlUtils.GetRuntimeStatus(msg.Event));
            record.SetSqlInt32(ColumnOrdinals.TaskID, SqlUtils.GetTaskId(msg.Event));
            record.SetDateTime(ColumnOrdinals.VisibleTime, SqlUtils.GetVisibleTime(msg.Event));

            SqlString reason = SqlUtils.GetReason(msg.Event);
            record.SetSqlString(ColumnOrdinals.Reason, reason);
            SqlString payloadText = SqlUtils.GetPayloadText(msg.Event);
            record.SetSqlString(ColumnOrdinals.PayloadText, payloadText);

            // If the message contains a payload (including Reason data), then we generate a random payload ID for it.
            // We could have done this in the stored procedure directly, but doing in in C# is quite a bit simpler.
            record.SetSqlGuid(
                ColumnOrdinals.PayloadId,
                payloadText.IsNull && reason.IsNull ? SqlGuid.Null : new SqlGuid(Guid.NewGuid()));

            return record;
        }
    }
}
