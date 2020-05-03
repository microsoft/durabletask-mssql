namespace DurableTask.SqlServer.AzureFunctions
{
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    class SqlDurabilityProvider : DurabilityProvider
    {
        public SqlDurabilityProvider(SqlServerOrchestrationService service, string connectionName)
            : base("SQL Server", service, service, connectionName)
        {
        }

        public SqlDurabilityProvider(
            SqlServerOrchestrationService service,
            IOrchestrationServiceClient client,
            string connectionName)
            : base("SQL Server", service, client, connectionName)
        {
        }
    }
}
