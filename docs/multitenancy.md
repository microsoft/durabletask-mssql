# Multitenancy

This article describes the multitenancy features of the Durable Task SQL backend and how to enable them.

## Overview

One of the goals for the Microsoft SQL provider for the Durable Task Framework (DTFx) is to enable [multi-tenant deployments](https://en.wikipedia.org/wiki/Multitenancy) with multiple apps sharing the same database. This is often valuable when your organization has many small apps but prefers to manage only a single backend database. When multitenancy is enabled, different apps connect to a shared database using different database login credentials. Database administrators will be able to query data across all tenants but individual apps will only have access to their own data.

Multitenancy works by isolating each app into a separate [task hub](taskhubs.md). The current task hub is determined by the credentials used to log into the database. For example, if your app connects to a Microsoft SQL database using **dbo** credentials (the default, built-in admin user for most databases), then the name of the connected task hub will be "dbo". Task hubs provide data isolation, ensuring that two users in the same database will not be able to access each other's data.

Another layer of multitenancy is added by the multi-schema support. This increases reliability and security for multiple services that are independently deployed in the same database, allowing each service control over their own schema and further isolation between service's data.

?> Task hub isolation in the current version of the SQL provider prevents one tenant from accessing data that belongs to another tenant. However, it doesn't impose any restrictions on data volumes or database CPU usage. If this kind of strict resource isolation is required, then each tenant should instead be separated into their own database.

## Managing custom schemas

For self-hosted DTFx app that opt for custom schema name, you can configure the schema name directly in the `SqlOrchestrationServiceSettings` class.

```csharp
var settings = new SqlOrchestrationServiceSettings
{
    SchemaName = "customSchemaName",
    TaskHubConnectionString = Environment.GetEnvironmentVariable("SQLDB_Connection"),
};
```

If no schema name name is explicitly configured, the default value `dt` will be used. Note that changing the value requires a restart of the app for the change to take effect.


## Enabling multitenancy

Multitenancy is enabled by default. In this mode, database administrators provide individual app owners with SQL credentials known only to them, and each credential maps to an isolated task hub within the database.

When using multitenacy mode, you do not (and should not) configure a task hub name in code or configuration. Instead, the SQL login username (from the [`USER_NAME()`](https://docs.microsoft.com/sql/t-sql/functions/user-name-transact-sql) SQL function) is automatically used as the task hub name (for example, `dbo`).

The following T-SQL can be used to _disable_ multitenancy:

```sql
-- Disable multi-tenancy mode
EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=0
```

The value `0` instructs all runtime stored procedures to instead infer the current task hub from the [`APP_NAME()`](https://docs.microsoft.com/sql/t-sql/functions/app-name-transact-sql) SQL function. The configured connection string is automatically modified to ensure that `APP_NAME()` is set to the explicitly configured name of the task hub, or `default` if no task hub name is configured.

Multitenancy can be re-enabled using the following T-SQL:

```sql
-- Enable multi-tenancy mode
EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=1
```

!> Enabling or disabling multitenancy may result in subsequent logins using a different task hub name. Any orchestrations or entities created using a previous task hub names will not be visible to an app that switches to a new task hub name. Switching between task hub modes must therefore be done with careful planning and should not be done while apps are actively running.

## Managing user credentials

Once multitenancy is enabled, each tenant must be given its own login and user ID for the target database. To ensure that each tenant can only access its own data, you should add each user to the `dt_runtime` role that is created automatically by the database setup scripts.

The following SQL statements illustrate how this can be done for a SQL database that supports username/password authentication.

```sql
-- create the new login credentials
CREATE LOGIN {login_name} WITH PASSWORD = {pw}
GO

-- create a user account associated with the new login credentials
CREATE USER {username} FOR LOGIN {login_name}
GO

-- add the user to the restricted dt_runtime role
ALTER ROLE dt_runtime ADD MEMBER {username}
GO
```

Each tenant should then use a SQL connection string with the above login credentials for their assigned user account. See [this SQL Server documentation](https://docs.microsoft.co/sql/relational-databases/security/authentication-access/create-a-database-user) for more information about how to create and manage database users.

?> Task hub names are limited to 50 characters. When multitenancy is enabled, the username is used as the task hub name. If the username exceeds 50 characters, the task hub name value used in the database will be a truncated version of the username followed by an MD5 hash of the full username.
