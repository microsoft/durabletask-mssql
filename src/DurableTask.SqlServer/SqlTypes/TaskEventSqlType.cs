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

    static class TaskEventSqlType
    {
        const string SqlTypeName = "dt.TaskEvents";

        static readonly SqlMetaData[] TaskEventSchema = new SqlMetaData[]
        {
            // IMPORTANT: Must be kept in sync with the database schema
            new SqlMetaData("InstanceID", SqlDbType.VarChar, 100),
            new SqlMetaData("ExecutionID", SqlDbType.VarChar, 50),
            new SqlMetaData("Name", SqlDbType.VarChar, 300),
            new SqlMetaData("EventType", SqlDbType.VarChar, 40),
            new SqlMetaData("TaskID", SqlDbType.Int),
            new SqlMetaData("VisibleTime", SqlDbType.DateTime2),
            new SqlMetaData("LockedBy", SqlDbType.VarChar, 100),
            new SqlMetaData("LockExpiration", SqlDbType.DateTime2),
            new SqlMetaData("Reason", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadText", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadID", SqlDbType.UniqueIdentifier),
        };

        static class ColumnOrdinals
        {
            // IMPORTANT: Must be kept in sync with the database schema
            public const int InstanceID = 0;
            public const int ExecutionID = 1;
            public const int Name = 2;
            public const int EventType = 3;
            public const int TaskID = 4;
            public const int VisibleTime = 5;
            public const int LockedBy = 6;
            public const int LockExpiration = 7;
            public const int Reason = 8;
            public const int PayloadText = 9;
            public const int PayloadId = 10;
        }

        public static SqlParameter AddTaskEventsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            IList<TaskMessage> outboundMessages)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;
            param.Value = ToTaskMessagesParameter(outboundMessages);
            return param;
        }

        public static SqlParameter AddTaskEventsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            TaskMessage message)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;
            param.Value = ToTaskMessageParameter(message);
            return param;
        }

        static IEnumerable<SqlDataRecord>? ToTaskMessagesParameter(
            this IEnumerable<TaskMessage> messages)
        {
            if (!messages.Any())
            {
                return null;
            }

            return GetTaskEventRecords();

            // Using a local function to support using null and yield syntax in the same method
            IEnumerable<SqlDataRecord> GetTaskEventRecords()
            {
                var record = new SqlDataRecord(TaskEventSchema);
                foreach (TaskMessage msg in messages)
                {
                    yield return PopulateTaskMessageRecord(msg, record);
                }
            }
        }

        static IEnumerable<SqlDataRecord> ToTaskMessageParameter(TaskMessage msg)
        {
            var record = new SqlDataRecord(TaskEventSchema);
            yield return PopulateTaskMessageRecord(msg, record);
        }

        static SqlDataRecord PopulateTaskMessageRecord(TaskMessage msg, SqlDataRecord record)
        {
            record.SetSqlString(ColumnOrdinals.InstanceID, msg.OrchestrationInstance.InstanceId);
            record.SetSqlString(ColumnOrdinals.ExecutionID, msg.OrchestrationInstance.ExecutionId);
            record.SetSqlString(ColumnOrdinals.Name, SqlUtils.GetName(msg.Event));
            record.SetSqlString(ColumnOrdinals.EventType, msg.Event.EventType.ToString());
            record.SetSqlInt32(ColumnOrdinals.TaskID, SqlUtils.GetTaskId(msg.Event));
            record.SetDateTime(ColumnOrdinals.VisibleTime, SqlUtils.GetVisibleTime(msg.Event));

            SqlString reasonText = SqlUtils.GetReason(msg.Event);
            record.SetSqlString(ColumnOrdinals.Reason, reasonText);
            SqlString payloadText = SqlUtils.GetPayloadText(msg.Event);
            record.SetSqlString(ColumnOrdinals.PayloadText, payloadText);

            // If the message contains a payload, then we generate a random payload ID for it.
            record.SetSqlGuid(
                ColumnOrdinals.PayloadId,
                payloadText.IsNull && reasonText.IsNull ? SqlGuid.Null : new SqlGuid(Guid.NewGuid()));

            // Optionally, the LockedBy and LockExpiration fields can be specified
            // to pre-lock task work items for this particular node.

            return record;
        }
    }
}
