namespace DurableTask.SqlServer.AzureFunctions
{
    using DurableTask.Core;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json.Linq;

    class SqlDurabilityProvider : DurabilityProvider
    {
        readonly SqlDurabilityOptions options;

        public SqlDurabilityProvider(
            SqlServerOrchestrationService service,
            SqlDurabilityOptions options)
            : base("SQL Server", service, service, options.ConnectionStringName)
        {
            this.options = options;
        }

        public SqlDurabilityProvider(
            SqlServerOrchestrationService service,
            SqlDurabilityOptions options,
            IOrchestrationServiceClient client)
            : base("SQL Server", service, client, options.ConnectionStringName)
        {
            this.options = options;
        }

        public override JObject ConfigurationJson => JObject.FromObject(this.options);
    }
}
