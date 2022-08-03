// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.SqlTypes
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlTypes;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;

    static class HistoryEventSqlType
    {
        static readonly SqlMetaData[] HistoryEventSchema = new SqlMetaData[]
        {
            // IMPORTANT: The order and schema of these items must always match the order of the SQL type in logic.sql
            new SqlMetaData("InstanceID", SqlDbType.VarChar, 100),
            new SqlMetaData("ExecutionID", SqlDbType.VarChar, 50),
            new SqlMetaData("SequenceNumber", SqlDbType.BigInt),
            new SqlMetaData("EventType", SqlDbType.VarChar, 40),
            new SqlMetaData("Name", SqlDbType.VarChar, 300),
            new SqlMetaData("RuntimeStatus", SqlDbType.VarChar, 30),
            new SqlMetaData("TaskID", SqlDbType.Int),
            new SqlMetaData("Timestamp", SqlDbType.DateTime2),
            new SqlMetaData("IsPlayed", SqlDbType.Bit),
            new SqlMetaData("VisibleTime", SqlDbType.DateTime2),
            new SqlMetaData("Reason", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadText", SqlDbType.VarChar, -1 /* max */),
            new SqlMetaData("PayloadID", SqlDbType.UniqueIdentifier),
            new SqlMetaData("ParentInstanceID", SqlDbType.VarChar, 100),
            new SqlMetaData("Version", SqlDbType.VarChar, 100),
        };

        static class ColumnOrdinals
        {
            // IMPORTANT: Must be kept in sync with the schema definition above
            public const int InstanceID = 0;
            public const int ExecutionID = 1;
            public const int SequenceNumber = 2;
            public const int EventType = 3;
            public const int Name = 4;
            public const int RuntimeStatus = 5;
            public const int TaskID = 6;
            public const int Timestamp = 7;
            public const int IsPlayed = 8;
            public const int VisibleTime = 9;
            public const int Reason = 10;
            public const int PayloadText = 11;
            public const int PayloadID = 12;
            public const int ParentInstanceID = 13;
            public const int Version = 14;
        };

        public static SqlParameter AddHistoryEventsParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            IEnumerable<HistoryEvent> newEventCollection,
            OrchestrationInstance instance,
            int nextSequenceNumber,
            EventPayloadMap eventPayloadMap,
            string schemaName)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = "HistoryEvents".PrependSchema(schemaName);
            param.Value = ToHistoryEventsParameter(newEventCollection, instance, nextSequenceNumber, eventPayloadMap);
            return param;
        }

        static IEnumerable<SqlDataRecord> ToHistoryEventsParameter(
            IEnumerable<HistoryEvent> historyEvents,
            OrchestrationInstance instance,
            int nextSequenceNumber,
            EventPayloadMap eventPayloadMap)
        {
            var record = new SqlDataRecord(HistoryEventSchema);
            foreach (HistoryEvent e in historyEvents)
            {
                record.SetSqlInt64(ColumnOrdinals.SequenceNumber, nextSequenceNumber++);
                record.SetSqlString(ColumnOrdinals.InstanceID, instance.InstanceId);
                record.SetSqlString(ColumnOrdinals.ExecutionID, instance.ExecutionId);
                record.SetSqlString(ColumnOrdinals.EventType, e.EventType.ToString());
                record.SetSqlString(ColumnOrdinals.Name, SqlUtils.GetName(e));
                record.SetSqlString(ColumnOrdinals.RuntimeStatus, SqlUtils.GetRuntimeStatus(e));
                record.SetSqlInt32(ColumnOrdinals.TaskID, SqlUtils.GetTaskId(e));
                record.SetDateTime(ColumnOrdinals.Timestamp, e.Timestamp);
                record.SetBoolean(ColumnOrdinals.IsPlayed, e.IsPlayed);
                record.SetDateTime(ColumnOrdinals.VisibleTime, SqlUtils.GetVisibleTime(e));

                if (eventPayloadMap.TryGetPayloadId(e, out Guid existingPayloadId))
                {
                    // We already have a payload saved in the DB for this event. Send only the payload ID.
                    record.SetSqlString(ColumnOrdinals.Reason, SqlString.Null);
                    record.SetSqlString(ColumnOrdinals.PayloadText, SqlString.Null);
                    record.SetSqlGuid(ColumnOrdinals.PayloadID, existingPayloadId);
                }
                else
                {
                    // This path is expected for ExecutionCompleted, possibly others?
                    SqlString reason = SqlUtils.GetReason(e);
                    record.SetSqlString(ColumnOrdinals.Reason, reason);
                    SqlString payload = SqlUtils.GetPayloadText(e);
                    record.SetSqlString(ColumnOrdinals.PayloadText, payload);
                    SqlGuid newPayloadId = reason.IsNull && payload.IsNull ? SqlGuid.Null : new SqlGuid(Guid.NewGuid());
                    record.SetSqlGuid(ColumnOrdinals.PayloadID, newPayloadId);
                }

                yield return record;
            }
        }
    }
}
