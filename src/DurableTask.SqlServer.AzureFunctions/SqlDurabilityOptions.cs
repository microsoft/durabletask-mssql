namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using Newtonsoft.Json;

    public class SqlDurabilityOptions
    {
        [JsonProperty("connectionStringName")]
        public string ConnectionStringName { get; set; } = "SQLDB_Connection";

        [JsonProperty("taskEventLockTimeout")]
        public TimeSpan TaskEventLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        internal SqlProviderOptions ProviderOptions { get; set; } = new SqlProviderOptions();
    }
}
