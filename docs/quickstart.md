# Quickstart Guide

## Azure Functions

For local development using Azure Functions, select one of the [tools available for local development](https://docs.microsoft.com/azure/azure-functions/functions-develop-local). To configure the Durable SQL provider, you'll need to add the [Microsoft.DurableTask.SqlServer.AzureFunction](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer.AzureFunctions) NuGet package reference to your project.

!> At the time of writing, the Durable SQL provider is in its early stages and does not yet work with the Azure Functions Consumption plan. It does work with the Azure Functions Elastic Premium plan but you must enable [Runtime Scale Monitoring](https://docs.microsoft.com/azure/azure-functions/functions-networking-options#premium-plan-with-virtual-network-triggers) to get automatic scaling. App Service plans are also supported. Consumption plan support and Scale Controller support for Elastic Premium is coming in a later release.

### .NET Core 3.1

Durable Functions projects targeting .NET Core 3.1 can add the [Microsoft.DurableTask.SqlServer.AzureFunction](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer.AzureFunctions) package by running the following `dotnet` CLI command:

```bash
dotnet add package Microsoft.DurableTask.SqlServer.AzureFunctions --prerelease
```

!> Durable Functions is not currently supported in the .NET 5 language worker.

### JavaScript, Python, and PowerShell

JavaScript, Python, and PowerShell projects can add the [Microsoft.DurableTask.SqlServer.AzureFunction](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer.AzureFunctions) package by running the following `func` CLI command. Note that in addition to the Azure Functions Core Tools, you must also have a recent [.NET SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1) installed locally.

```bash
func extensions install -p Microsoft.DurableTask.SqlServer.AzureFunctions -v 0.9.1-beta
```

?> Check [here](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer.AzureFunctions) to see if newer versions of the SQL provider package are available, and update the above command to reference the latest available version.

!> The Durable SQL backend is not currently supported with extension bundles. Support for extension bundles will be available at or before the *General Availability* release.

### Host.json configuration

You can configure the Durable SQL provider by updating the `extensions/durableTask/storageProvider` section of your **host.json** file.

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type": "mssql",
        "connectionStringName": "SQLDB_Connection",
        "taskEventLockTimeout": "00:02:00"
      }
    }
  }
}
```

The `"type": "mssql"` specification is required to inform the Durable Functions extension that it should use the SQL backend instead of the default Azure Storage backend.

The `connectionStringName` setting is required and must be set to the name of the app setting or environment variable that contains your SQL connection string. In the above example, `SQLDB_Connection` is the name of an existing app setting or environment variable. If you're running locally and using a **local.settings.json** file, you can configure it as follows:

```json
{
  "IsEncrypted": false,
  "Values": {
    "SQLDB_Connection": "Server=localhost;Database=DurableDB;..."
  }
}
```

To enable diagnostic logging, you can also add the following `logging` configuration to your **host.json** file:

```json
{
  "logging": {
    "logLevel": {
      "DurableTask.SqlServer": "Information",
      "DurableTask.Core": "Warning"
    }
  }
}
```

It is recommended to always configure the `DurableTask.*` log categories to at least `Warning` to ensure you have visibility into issues that might impact reliability and runtime performance.

?> By default, task hub names configured in host.json are ignored. Instead, the task hub name is inferred from the SQL database login username. For more information, see the [Task Hubs](taskhubs.md) documentation.

## Self-hosted .NET apps

The Durable SQL provider can also be used by Durable Task Framework (DTFx) apps targeting .NET Standard 2.0 and above. To reference the Durable SQL provider package for DTFx, uses the `dotnet` CLI to add a reference to the [Microsoft.DurableTask.SqlServer](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer/) package.

```bash
dotnet add package Microsoft.DurableTask.SqlServer --prerelease
```

If you're already familiar with creating apps using the Durable Task Framework in .NET, you can simply initialize an instance of the [SqlOrchestrationService](src/DurableTask.SqlServer/SqlOrchestrationService.cs) class and pass it as a parameter to the `TaskHubWorker` and `TaskHubClient` objects.

```csharp
var settings = new SqlOrchestrationServiceSettings
{
    ConnectionString = "Server=localhost;Database=DurableDB;...",
};

// This console logging requires Microsoft.Extensions.Logging.Console v5.0.0 or greater
ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSimpleConsole(options => options.SingleLine = true);
});

var provider = new SqlOrchestrationService(settings);

// Install the DB schema, if necessary
await provider.CreateIfNotExistsAsync();

var worker = await new TaskHubWorker(provider, loggerFactory);
worker.AddTaskOrchestrations(/* ... */);
worker.AddTaskActivities(/* ... */);
await worker.StartAsync();

var client = new TaskHubClient(provider, loggerFactory: loggerFactory);
await client.CreateOrchestrationInstanceAsync(/* ... */);
```

See the [Durable Task Framework wiki](https://github.com/Azure/durabletask/wiki) for more examples of how to write self-hosted DTFx apps.

## Database setup

If you're not already familiar with how to set up a [Microsoft SQL Server](https://www.microsoft.com/sql-server/) database, the following are a few options for getting started. You can skip these instructions if you already have a SQL database available.

### SQL Server on Windows

If you're running on Windows 10 or Windows Server, you can manually install SQL Server 2019 (or greater) directly on your machine. More information about how to download and install SQL Server can be found [here](https://www.microsoft.com/sql-server/sql-server-downloads).

?> If you plan on running the automated tests in the [SQL provider GitHub repo](https://github.com/microsoft/durabletask-mssql), then you will need to [enable SQL Server and Windows Authentication mode](https://docs.microsoft.com/sql/database-engine/configure-windows/change-server-authentication-mode) on your SQL installation. This setting allows your SQL Server installation to support both integrated Windows account logins and username/password logins.

Once SQL Server is installed, you can use [SQL Server Management Studio](https://docs.microsoft.com/sql/ssms) to create a database named `DurableDB`. Make sure to set the database collation to `Latin1_General_100_BIN2_UTF8`.

### Docker container

This is the easiest option for doing local development if you are using macOS or Linux. You can simply install [Docker](https://www.docker.com/products/docker-desktop) locally and run the following PowerShell commands:

```powershell
# primary parameters
$pw        = "yourStrong(!)Password"
$edition   = "Developer"
$port      = 1433
$tag       = "2019-latest"
$dbname    = "DurableDB"
$collation = "Latin1_General_100_BIN2_UTF8"

# pull the image from the Microsoft container registry
docker pull mcr.microsoft.com/mssql/server:$tag

# run the image, providing some basic setup parameters
docker run --name mssql-server \
    -e 'ACCEPT_EULA=Y' \
    -e "SA_PASSWORD=$pw" \
    -e "MSSQL_PID=$edition" \
    -p ${port}:1433 \
    -d mcr.microsoft.com/mssql/server:$tag

# wait a few seconds for the container to start...

# create the database with strict binary collation
docker exec -d mssql-server /opt/mssql-tools/bin/sqlcmd \
    -S . \
    -U sa \
    -P "$pw" \
    -Q "CREATE DATABASE [$dbname] COLLATE $collation"
```

You should now have a docker container hosting SQL Server and listening on the port **1433** (the default port for SQL Server). You can change the `$port` number to some other value if you already have another process listening on port 1433.

### Azure SQL database

An Azure SQL database is the best option if you're planning on hosting your app in Microsoft's Azure cloud. Microsoft offers a [12-month free Azure subscription account](https://azure.microsoft.com/free/) if you're exploring Azure for the first time.

The following are the [Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli) commands you can run to create your database. You can run these locally or use https://shell.azure.com for a browser-based Azure CLI experience.

```powershell
# Set variables for your server and database
$resourceGroupName = "myResourceGroup"
$location = "eastus"
$adminLogin = "myadminuser"
$password = "myStrong(!)Password"
$serverName = "mysqlserver123"
$databaseName = "DurableDB"

# The ip address range that you want to allow to access your server
$startIp = "0.0.0.0"
$endIp = "0.0.0.0"

echo "Resource group name is $resourceGroupName"
echo "Server name is $serverName"

# Create an Azure resource group for your database
az group create --name $resourceGroupName --location $location

# Create a SQL server and configure admin credentials
az sql server create \
    --name $serverName \
    --resource-group $resourceGroupName \
    --location $location  \
    --admin-user $adminlogin \
    --admin-password $password

# Configure client IP address restrictions for the SQL server
az sql server firewall-rule create \
    --resource-group $resourceGroupName \
    --server $serverName \
    -n AllowYourIp \
    --start-ip-address $startip \
    --end-ip-address $endip

# Finally, create the database itself
az sql db create \
    --resource-group $resourceGroupName \
    --server $serverName \
    --name $databaseName \
    --collation Latin1_General_100_BIN2_UTF8 \
    --edition GeneralPurpose \
    --compute-model Serverless \
    --family Gen5 \
    --capacity 2
```

If you prefer to use the Azure portal, you can alternatively follow [this Azure portal quickstart](https://docs.microsoft.com/azure/azure-sql/database/single-database-create-quickstart?tabs=azure-portal) to learn how to get started with Azure SQL database. When configuring the database in the portal, make sure to set the **Database collation** (under *Additional settings*) to `Latin1_General_100_BIN2_UTF8`.

### Provisioning database schemas

When the task hub worker for your app starts up, it will automatically check to see if the configured database has been updated with the latest DTFx schema. If the schema is not found, it will be provisioned on-demand. This automatic process also takes care of upgrading schemas if older versions are found in the database.

> If you would prefer to install the SQL schema manually, you can do so using the SQL scripts found [here](https://github.com/microsoft/durabletask-mssql/tree/main/src/DurableTask.SqlServer/Scripts).
