# Multitenancy

This article describes the multitenancy features of the Durable Task SQL backend and how to enable them.

## Overview

One of the goals for the Microsoft SQL provider for the Durable Task Framework (DTFx) is to enable [multi-tenant deployments](https://en.wikipedia.org/wiki/Multitenancy) with multiple apps sharing the same database. This is often valuable when your organization has many small apps but prefers to manage only a single backend database. When multitenancy is enabled, different apps connect to a shared database using different database login credentials. Database administrators will be able to query data across all tenants but individual apps will only have access to their own data.

Multitenancy works by isolating each app into a separate [task hub](taskhubs.md). The current task hub is determined by the credentials used to log into the database. For example, if your app connects to a Microsoft SQL database using **dbo** credentials (the default, built-in admin user for most databases), then the name of the connected task hub will be "dbo". Task hubs provide data isolation, ensuring that two users in the same database will not be able to access each other's data.

?> Task hub isolation in the current version of the SQL provider prevents one tenant from accessing data that belongs to another tenant. However, it doesn't impose any restrictions on data volumes or database CPU usage. If this kind of strict resource isolation is required, then each tenant should instead be separated into their own database.

## Enabling multitenancy

If you want to have multiple apps share a database (multitenancy) but want to ensure no app can access any data owned by another app, then you can configure a task hub via database login credentials. In this model, database administrators provide individual app owners with SQL credentials known only to them, and each credential maps to an isolated task hub within the database. When using this model, you do not configure a task hub name in code or configuration. Instead, the SQL login username is used as the task hub name.

Multitenancy is disabled by default. To enable multitenancy, a database administrator must set `TaskHubMode` to `1` in the `dt.GlobalSettings` table. This can be done using the `dt.SetGlobalSetting` stored procedure.

```sql
EXECUTE dt.SetGlobalSetting @Name='TaskHubMode', @Value=1
```

The value `1` instructs all runtime stored procedures to infer the current task hub from the [`USER_NAME()`](https://docs.microsoft.com/sql/t-sql/functions/user-name-transact-sql) function of SQL Server. Multitenancy can be disabled by setting `TaskHubMode` to `0`.

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
