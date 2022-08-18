# Multitenancy

This article describes the multitenancy features of the Durable Task SQL backend and how to enable them.

## Overview

One of the goals for the Microsoft SQL provider for the Durable Task Framework (DTFx) is to enable [multi-tenant deployments](https://en.wikipedia.org/wiki/Multitenancy) with multiple apps sharing the same database. This is often valuable when your organization has many small apps but prefers to manage only a single backend database. When multitenancy is enabled, different apps connect to a shared database using different database login credentials and each app will only have access to its own data.

Note that there are two modes for multi-tenancy, _shared schema_ mode and _isolated schema_ mode.

?> Multitenancy in the current version of the SQL provider prevents one tenant from accessing data that belongs to another tenant. However, it doesn't provide isolation for shared resources within a database, such as memory or CPU. If this kind of strict resource isolation is required, then each tenant should instead be separated into its own database.

## Shared schema mode

Shared schema multitenancy works by isolating each app into a separate [task hub](taskhubs.md). The current task hub is determined by the credentials used to log into the database. For example, if your app connects to a Microsoft SQL database using **dbo** credentials (the default, built-in admin user for most databases), then the name of the connected task hub will be "dbo". Task hubs provide data isolation, ensuring that two users in the same database will not be able to access each other's data.

Shared schema mode is available starting in the v1.0.0 version of the MSSQL storage provider. The benefit of this mode is that fewer database objects need to be created in the database. It also enables high-privileged user accounts to write SQL queries that span multiple tenants. The downside of this mode is that schema updates must be applied to all tenants at once, which increases the risk associated with schema upgrades.

### Enabling shared schema multitenancy

Shared schema multitenancy is enabled by default. When using shared schema multitenacy, you do not (and should not) configure a task hub name in code or configuration. Instead, the SQL login username (from the [`USER_NAME()`](https://docs.microsoft.com/sql/t-sql/functions/user-name-transact-sql) SQL function) is automatically used as the task hub name (for example, `dbo`).

The following T-SQL can be used to _disable_ shared schema multitenancy:

```sql
-- Disable multi-tenancy mode
EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=0
```

The value `0` instructs all runtime stored procedures to instead infer the current task hub from the [`APP_NAME()`](https://docs.microsoft.com/sql/t-sql/functions/app-name-transact-sql) SQL function. The configured connection string is automatically modified to ensure that `APP_NAME()` is set to the explicitly configured name of the task hub, or `default` if no task hub name is configured.

Shared schema multitenancy can be re-enabled using the following T-SQL:

```sql
-- Enable multi-tenancy mode
EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=1
```

!> Enabling or disabling shared schema multitenancy may result in subsequent logins using a different task hub name. Any orchestrations or entities created using a previous task hub names will not be visible to an app that switches to a new task hub name. Switching between task hub modes must therefore be done with careful planning and should not be done while apps are actively running.

## Isolated schema mode

Isolated schema mode provisions an independent set of database objects (tables, views, stored procedures, etc.) for each tenant. This increases reliability and security for multiple services that are independently deployed in the same database, allowing each service control over their own schema and further isolation between service's data. For example, it's also possible to provide a degree of storage isolation for tables of particular tenants using [SQL Server Filegroups](https://docs.microsoft.com/sql/relational-databases/databases/database-files-and-filegroups?view=sql-server-ver16) (note that there is no automatic support for this). Isolated schema mode also allows schema versions to be managed independently for each tenant.

Isolated schema mode is available starting in the v1.1.0 release of the MSSQL storage provider.

### Managing custom schemas

For Azure Functions apps, you can configure a custom schema name in the Azure Functions **host.json** file.

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type": "mssql",
        "connectionStringName": "SQLDB_Connection",
        "schemaName": "MyCustomSchemaName"
      }
    }
  }
}
```

For self-hosted DTFx app that opt for custom schema name, you can configure the schema name directly in the `SqlOrchestrationServiceSettings` constructor.

```csharp
var settings = new SqlOrchestrationServiceSettings(
    connectionString: Environment.GetEnvironmentVariable("SQLDB_Connection"),
    schemaName: "MyCustomSchemaName");
```

If no schema name name is explicitly configured, the default value `dt` will be used. Note that changing the value requires a restart of the app for the change to take effect.

## Managing user credentials

Once multitenancy is enabled, each tenant must be given its own login and user ID for the target database. To ensure that each tenant can only access its own data, you should add each user to the `{schema_name}_runtime` role that is created automatically by the database setup scripts. By default, this is `dt_runtime` since the default schema name is `dt`.

The following SQL statements illustrate how this can be done for a SQL database that supports username/password authentication.

```sql
-- create the new login credentials
CREATE LOGIN {login_name} WITH PASSWORD = {pw}
GO

-- create a user account associated with the new login credentials
CREATE USER {username} FOR LOGIN {login_name}
GO

-- add the user to the restricted dt_runtime role
ALTER ROLE {schema_name}_runtime ADD MEMBER {username}
GO
```

Each tenant should then use a SQL connection string with the above login credentials for their assigned user account. See [this SQL Server documentation](https://docs.microsoft.co/sql/relational-databases/security/authentication-access/create-a-database-user) for more information about how to create and manage database users.

?> Task hub names are limited to 50 characters. When multitenancy is enabled, the username is used as the task hub name. If the username exceeds 50 characters, the task hub name value used in the database will be a truncated version of the username followed by an MD5 hash of the full username.
