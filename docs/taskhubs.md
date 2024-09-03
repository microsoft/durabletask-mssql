# Task Hubs

This article describes what task hubs are and how they can be configured.

## Overview

A **task hub** is a logical grouping concept in both the Durable Task Framework (DTFx) and Durable Functions. Orchestrators, activities, and entities all belong to a single task hub and can only interact directly with other orchestrations, activities, and entities that are defined in the same task hub. In the SQL provider, a single database can contain multiple task hubs. Task hub data isolation is enforced at runtime by the underlying DTFx storage provider and its SQL stored procedures. In the case of the DTFx SQL provider, all stored procedures used by the runtime will only ever access data that belongs to the current task hub.

Task hubs are also the primary unit of isolation within a database. Each table in the Durable Task schema includes a `TaskHub` column as part of its primary key and stored procedures will only access data that belongs to the current _task hub context_. This isolation serves two primary purposes: supporting side-by-side deployments of different application version and [enabling multitenancy](multitenancy.md), as explained in other articles.

?> One difference between the Microsoft SQL provider and the Azure Storage provider is that all task hubs in the Microsoft SQL provider share the same tables. In the Azure Storage provider, each task hub is given a completely separate table in Azure Storage (along with isolated queues and blob containers). More importantly, however, is that the SQL provider allows task hubs to be securely isolated from each other. This is not possible with the Azure Storage provider - different tenants would need to be assigned to different storage accounts. The ability for multiple tenants to securely share a SQL databases is therefore much more cost-effective for implementing multitenancy.

## Configuring task hub names

By default, the name of a task hub is the name of the database user. No explicit configuration is required. For more information, see the [Multitenancy](multitenancy.md) topic.

Automatic task hub name inference can be disabled by disabling multitenancy in the database. When multitenancy is disabled, task hubs names can be configured explicitly in the SQL provider configuration, as shown in the following examples.

For Durable Functions apps, explicit task hub names are configured in the `extensions/durableTask/hubName` property of the **host.json** file.

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "hubName": "MyTaskHub",
      "storageProvider": {
        "type": "mssql",
        "connectionStringName": "SQLDB_Connection"
      }
    }
  }
}
```

For self-hosted DTFx apps that opt-out of multitenant mode, you can configure the task hub directly in the `SqlOrchestrationServiceSettings` class.

```csharp
var settings = new SqlOrchestrationServiceSettings
{
    TaskHubName = "MyTaskHub",
    TaskHubConnectionString = Environment.GetEnvironmentVariable("SQLDB_Connection"),
};
```

If no task hub name is explicitly configured, the value `default` will be used. Note that any task hub name configuration is ignored when the database is in multitenancy mode (which is the default behavior).

?> Task hub names are limited to 50 characters. If the specified task hub name exceeds 50 characters, it will be truncated and suffixed with an MD5 hash of the full task hub name to keep it within 50 characters. This behavior applies both to task hubs inferred from database usernames and explicitly configured task hub names.

## Case sensitivity

Whether task hub names are case-sensitive depends on the collation of the SQL database. For example, if a [binary collation](https://docs.microsoft.com/sql/relational-databases/collations/collation-and-unicode-support#Binary-collations) is configured on the database, task hub names will be case-sensitive. Non-binary collations may result in case-insensitive string comparisons, making task hub names effectively case-insensitive. For more information on SQL database collations, see [Collation and Unicode support](https://docs.microsoft.com/sql/relational-databases/collations/collation-and-unicode-support) in the Microsoft SQL documentation.

?> The preferred database collation for the Durable Task SQL provider is `Latin1_General_100_BIN2_UTF8`, which is a binary collation.
