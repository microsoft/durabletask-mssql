using System;
using System.Data.Common;
using System.Data.SqlClient;

namespace DurableTask.RelationalDb.SqlServer
{
    public class SqlServerProvider : RelationalDbOrchestrationService
    {
        public string ConnectionString { get; set; } = "Server=localhost;Database=TaskHub;Trusted_Connection=True;";

        // TODO: make these configurable
        public override int MaxActivityConcurrency => Environment.ProcessorCount;

        public override int MaxOrchestrationConcurrency => Environment.ProcessorCount;

        public override DbConnection GetConnection()
        {
            return new SqlConnection(this.ConnectionString);
        }
    }
}
