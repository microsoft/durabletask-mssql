# Quickstart Guide

## Database setup

This storage provider uses [Microsoft SQL Server](https://www.microsoft.com/sql-server/) to persist orchestration and entity state for Durable Functions and the Durable Task Framework. You have several options for getting started with SQL Server depending on the platform you want to target.

### Docker container

This is the easiest option for doing local development, especially if you are using macOS or Linux. You can simply install [Docker](https://www.docker.com/products/docker-desktop) locally and run the following PowerShell commands:

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

### SQL Server on Windows

If you're running on Windows 10 or Windows Server, you can manually install SQL Server 2019 (or greater) directly on your machine. More information about how to download and install SQL Server can be found [here](https://www.microsoft.com/sql-server/sql-server-downloads).

?> If you plan on running the automated tests in the [SQL provider GitHub repo](https://github.com/Azure/durabletask-mssql), then you will need to [enable SQL Server and Windows Authentication mode](https://docs.microsoft.com/sql/database-engine/configure-windows/change-server-authentication-mode) on your SQL installation. This setting allows your SQL Server installation to support both integrated Windows account logins and username/password logins.

Once SQL Server is installed, you can use [SQL Server Management Studio](https://docs.microsoft.com/sql/ssms) to create a database named `DurableDB`. Make sure to set the database collation to `Latin1_General_100_BIN2_UTF8`.

### Provisioning database schemas

When the task hub worker for your app starts up, it will automatically check to see if the configured database has been updated with the latest DTFx schema. If the schema is not found, it will be provisioned on-demand. This automatic process also takes care of upgrading schemas if older versions are found in the database.

> If you would prefer to install the SQL schema manually, you can do so using the SQL scripts found [here](https://github.com/Azure/durabletask-mssql/tree/main/src/DurableTask.SqlServer/Scripts).
>
> * **schema-{version}.sql**: This script provisions the database schema, tables, and custom table types. The `{version}` corresponds to the schema version (0.4.0-alpha, 0.5.0-beta, 1.0.0, etc.). All `schema-{version}.sql` scripts must be run in-order since each version builds on top of the previous version.
> * **logic.sql**: This script provisions the stored procedures and views that are used to interact with the database. This script must be run after all `schema-{version}.sql` scripts are successfully executed.
> * **drop-schema.sql**: This script can be used to drop all database artifacts used by the SQL provider. Use this with extreme caution since it will permanently delete all data associated with the Durable SQL Provider. It will not attempt to delete any other schemas or data *not* associated with the Durable SQL provider.
>
> Each of the above scripts are idempotent and are safe to run multiple successive times.

## Azure Functions

For local development using Azure Functions, select one of the [tools available for local development](https://docs.microsoft.com/azure/azure-functions/functions-develop-local). To configure the Durable SQL provider, you'll need to add the [DurableTask.SqlServer.AzureFunction](https://www.myget.org/feed/azure-appservice/package/nuget/DurableTask.SqlServer.AzureFunctions) NuGet package reference to your project.

!> At the time of writing, the Durable SQL provider is in its early stages and does not yet work with the Azure Functions Consumption plan. It does work with the Azure Functions Elastic Premium plan if you manually configure a minimum number of instances for your app, but does not support automatic scaling. App Service plans with pre-configured instance counts are also supported today. Consumption plan support and automatic, elastic scaling is coming in a later release.

### .NET Core 3.1

Durable Functions projects targeting .NET Core 3.1 can add the **DurableTask.SqlServer.AzureFunction** package by updating running the following `dotnet` CLI command:

```bash
dotnet add package DurableTask.SqlServer.AzureFunctions \
    --prerelease \
    --source https://www.myget.org/F/azure-appservice/api/v3/index.json
```

!> Durable Functions is not currently supported in the .NET 5 language worker.

### JavaScript, Python, and PowerShell

JavaScript, Python, and PowerShell projects can add the **DurableTask.SqlServer.AzureFunction** package by updating running the following `func` CLI command. Note that in addition to the Azure Functions Core Tools, you must also have a recent [.NET SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1) installed locally.

```bash
dotnet add package DurableTask.SqlServer.AzureFunctions \
    --prerelease \
    --source https://www.myget.org/F/azure-appservice/api/v3/index.json
```

!> The Durable SQL backend is not currently supported with extension bundles. Support for extension bundles will be available at or before the *General Availability* release.

### Host.json configuration

You can configure the Durable SQL provider by updating the `extensions/durableTask/storageProvider` section of your **host.json** file.

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "connectionStringName": "SQLDB_Connection",
        "taskEventLockTimeout": "00:02:00"
      }
    }
  }
}
```

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

## Self-hosted .NET apps

The Durable SQL provider can also be used by Durable Task Framework (DTFx) apps targeting .NET Core 3.1 and above. To reference the Durable SQL provider package for DTFx, uses the `dotnet` CLI to add a reference to the [DurableTask.SqlServer](https://www.myget.org/feed/azure-appservice/package/nuget/DurableTask.SqlServer) package.

```bash
dotnet add package DurableTask.SqlServer \
    --prerelease \
    --source https://www.myget.org/F/azure-appservice/api/v3/index.json
```

If you're already familiar with creating apps using the Durable Task Framework in .NET, you can simply initialize an instance of the [SqlOrchestrationService](src/DurableTask.SqlServer/SqlOrchestrationService.cs) class and pass it as a parameter to the `TaskHubWorker` and `TaskHubClient` objects.

```csharp
var options = = new SqlProviderOptions
{
    ConnectionString = "Server=localhost;Database=DurableDB;...",
};

var provider = new SqlOrchestrationService(options);
await provider.CreateIfNotExistsAsync();

var worker = await new TaskHubWorker(provider);
worker.AddTaskOrchestrations(/* ... */);
worker.AddTaskActivities(/* ... */);
await worker.StartAsync();

var client = new TaskHubClient(provider);
await client.CreateOrchestrationInstanceAsync(/* ... */);
```

 See the [Durable Task Framework wiki](https://github.com/Azure/durabletask/wiki) for more examples of how to write self-hosted DTFx apps.
