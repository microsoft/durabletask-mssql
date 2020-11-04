namespace DurableTask.SqlServer.SqlTypes
{
    using System.Collections.Generic;
    using System.Data;
    using DurableTask.Core;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;

    static class MessageIdSqlType
    {
        const string SqlTypeName = "dt.MessageIDs";

        static readonly SqlMetaData[] MessageIdSchema = new SqlMetaData[]
        {
            // IMPORTANT: The order and schema of these items must always match the order of the SQL type in logic.sql
            new SqlMetaData("InstanceID", SqlDbType.VarChar, 100),
            new SqlMetaData("SequenceNumber", SqlDbType.BigInt),
        };

        public static SqlParameter AddMessageIdParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            IEnumerable<TaskMessage> messageCollection)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;
            param.Value = ToMessageIDsParameter(messageCollection);
            return param;
        }

        public static SqlParameter AddMessageIdParameter(
            this SqlParameterCollection commandParameters,
            string paramName,
            TaskMessage message)
        {
            SqlParameter param = commandParameters.Add(paramName, SqlDbType.Structured);
            param.TypeName = SqlTypeName;
            param.Value = ToMessageIDsParameter(message);
            return param;
        }

        static IEnumerable<SqlDataRecord> ToMessageIDsParameter(IEnumerable<TaskMessage> messages)
        {
            var record = new SqlDataRecord(MessageIdSchema);
            foreach (TaskMessage message in messages)
            {
                yield return PopulateMessageIdRecord(message, record);
            }
        }

        static IEnumerable<SqlDataRecord> ToMessageIDsParameter(TaskMessage message)
        {
            var record = new SqlDataRecord(MessageIdSchema);
            yield return PopulateMessageIdRecord(message, record);
        }

        static SqlDataRecord PopulateMessageIdRecord(TaskMessage message, SqlDataRecord record)
        {
            // IMPORTANT: The order of these columns must always match the order of the SQL type
            record.SetString(0, message.OrchestrationInstance.InstanceId);
            record.SetInt64(1, message.SequenceNumber);
            return record;
        }
    }
}
