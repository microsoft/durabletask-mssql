namespace DurableTask.SqlServer.AzureFunctions
{
    public class SqlDurabilityOptions
    {
        public string ConnectionStringName { get; set; } = "SQLDB_Connection";

        public SqlServerProviderOptions ProviderOptions { get; } = new SqlServerProviderOptions();
    }
}
