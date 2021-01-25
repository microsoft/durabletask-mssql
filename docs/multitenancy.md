# Multitenancy

One of the goals for the Microsoft SQL provider for the Durable Task Framework (DTFx) is to create a foundation for safe [multi-tenant deployments](https://en.wikipedia.org/wiki/Multitenancy). This is especially valuable when your organization has many small apps but prefers to manage only a single backend database. Different apps can connect to this database using different database login credentials. Database administrators will be able to query data across all tenants but individual apps will only have access to their own data.

## Task hubs

A **task hub** is an abstract grouping concept in DTFx and Durable Functions. Orchestrators, activities, and entities can only interact with each other when they belong to the same task hub. This is enforced at runtime by the underlying DTFx storage provider. In the case of the DTFx SQL provider, all stored procedures used by the runtime will only ever access data that belongs to the current task hub.

The current task hub is determined by the credentials used to log into the database. For example, if your app connects to a Microsoft SQL database using **dbo** credentials (the default, built-in admin user for most databases), then the name of the connected task hub will be "dbo". It is not necessary to explicitly create or delete task hubs. All orchestrations and entities created under that connection will automatically be associated with the corresponding task hub.

?> One difference between the Microsoft SQL provider and the Azure Storage provider is that all task hubs in the Microsoft SQL provider share the same tables. In the Azure Storage provider, each task hub is given a completely separate table in Azure Storage (along with isolated queues and blob containers). More importantly, however, is that the SQL provider allows task hubs to be securely isolated from each other. This is not possible with the Azure Storage provider - different tenants would need to be assigned to different storage accounts. The ability for multiple tenants to securely share a SQL databases is therefore much more cost-effective for implementing multitenancy.

Each table in the Durable Task schema includes a `TaskHub` column that indicates the name of the tenant that a particular row belongs to. The stored procedures used to access data in the database will always filter data using the current task hub context. This ensures that each credential can only access data that is part of the same task hub. The task hub is also the first component in all primary keys within the database and is thus part of the identity of all instances.

## Getting started

To enable multitenancy, each tenant must be given its own login and user ID for the target database. To ensure that each tenant can only access its own data, you should add each user to the `dt_runtime` role that is created automatically by the setup scripts using the following T-SQL syntax.

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

!> Task hub names are limited to 50 characters. Database username lengths must therefore not exceed 50 characters.
