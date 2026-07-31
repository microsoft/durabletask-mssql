
# Database Schema Scripts

This README describes the database schema scripts used by the Durable Task MSSQL storage provider and how to make changes to the database schema.

## Overview

This directory contains the scripts used to create and update the database schema for the Durable Task MSSQL storage provider.
The following scripts are provided:

* `schema-x.y.z.sql`: Creates or updates the database table schema objects.
* `logic.sql`: Creates or updates the database stored procedures.
* `permissions.sql`: Grants permissions to the Durable Task extension to access the schema objects.
* `drop-schema.sql`: Drops the Durable Task schema and all objects in it from the database.

These script files are embedded directly into the `DurableTask.SqlServer.dll` assembly and are executed as necessary to either create
the MSSQL database schema or upgrade it to the latest version.

## Schema Versioning

The database schema is versioned using the [Semantic Versioning](https://semver.org/) scheme. The version number is stored in the `Versions` table.
If a database has gone through one or more schema upgrades, then each previous version number will be stored in the `Versions` table, providing a kind of update audit trail.

The actual version number stored in the `Versions` table is based on the nuget package version. However, new version rows are only added when new `schema-x.y.z.sql` scripts are added to the project.

## Schema Upgrade Process

The Durable Task extension uses the following process to upgrade the database schema:

1. If the `Versions` table does not exist, then the `schema-1.0.0.sql` script is executed to create the schema.
1. If the `Versions` table exists, then the latest version number is read from the table.
1. If there are any schema scripts with a version number greater than the latest version number, then the scripts are executed in order to upgrade the schema.
1. After all the schema scripts have been executed, the `logic.sql` script is executed to create or update the stored procedures.
1. Finally, the `permissions.sql` script is executed to grant permissions to the Durable Task extension.

In Azure Durable Functions, this process happens each time the application starts running, so the database schema will always be up-to-date with the latest version of the Durable Task extension.
When using the Durable Task Framework directly, this process happens when `CreateAsync` or `CreateIfNotExistsAsync` methods of `SqlOrchestrationService` are called.

## Changing Database Schema

The existing `schema-x.y.z.sql` files generally should NOT be modified after they are published. If you need to make changes to the database schema, follow these steps:

1. Create a new `schema-x.y.z.sql` script file with the new schema. The `x.y.z` numbers should match the new nuget package version that will be shipped with this new script.
1. Copy/paste the generic warning comments from any existing `schema-x.y.z.sql` files into the new script file as appropriate.
1. For adding new columns or indexes, use the appropriate `ALTER TABLE` statements as well as the `IF NOT EXISTS` syntax to avoid errors if the column or index already exists. These scripts must be safe to run multiple times.
1. Do not change an existing user-defined table type used as a table-valued parameter (TVP) in a minor or patch release. Published TVPs are immutable client/database contracts. Adding even a nullable column causes SQL Server to reject records sent by older clients before the stored procedure begins executing.

### Table-valued parameter compatibility

Multiple Function Apps can share one database schema while being deployed independently. The first app running a newer provider version automatically upgrades the shared schema. Apps that have not upgraded must still be able to submit their existing TVP shapes.

The published `InstanceIDs`, `MessageIDs`, `HistoryEvents`, `OrchestrationEvents`, and `TaskEvents` types must therefore not have columns added, removed, reordered, renamed, or changed outside an explicitly planned major/breaking release. A SQL guard inside a stored procedure cannot make a changed TVP compatible because SQL Server validates the parameter shape before executing the procedure.

When new tabular input is required, create a versioned TVP and a versioned stored procedure entry point, and retain the previous versions for rolling upgrades. Prefer a scalar stored procedure parameter when it can carry the new value without changing a TVP. Do not add runtime metadata checks or conversion paths to hot procedures; versioning preserves compatibility without adding per-operation overhead.

The `PublishedTableValuedParameterContractsRemainCompatible` integration test submits frozen, previously published TVP record shapes to the latest database schema. Its expected shapes are a compatibility baseline and must not be updated to accommodate a minor or patch schema change.

## Changing Stored Procedures or Permissions

The `logic.sql` and `permissions.sql` files can be modified as needed. These files are not versioned and are executed every time the application starts (Azure Durable Functions) or every time `CreateAsync` or `CreateIfNotExistsAsync` is called (Durable Task Framework).

## Testing Schema Changes

There are several tests which validate the database schema included in this project. Most of these tests are in the [`DatabaseManagement.cs`](../../../test/DurableTask.SqlServer.Tests/Integration/DatabaseManagement.cs) file.
When making schema changes, some of those tests will fail and will need to be updated. Some updates include:

* Multiple test methods (`CanCreateAndDropSchema`, `CanCreateAndDropMultipleSchemas`, `CanCreateIfNotExists`, etc.) will need to be updated to list the new `schema-x.y.z.sql` script file name.
* The `ValidateDatabaseSchemaAsync` method will need to be updated to check for the newest schema version number.

## Testing Database Upgrades

The [UpgradeTests.cs](../../../test/DurableTask.SqlServer.Tests/Integration/UpgradeTests.cs) file contains tests which validate that the database schema upgrade process works correctly.
It works by:

* Restoring a backed-up database based on the `1.0.0` schema to a local SQL Server instance.
* Starting an app that requires a newer schema version to trigger an automatic schema upgrade.
* Runs a mix of new and old orchestrations to ensure that the schema upgrade was successful and that no data was lost.

This test is critical to ensure that end-users won't be negatively impacted by schema changes.
Unfortunately, it is not possible to run this test in CI yet until additional changes are made to support restoring database backups using Docker containers.
However, it can be run locally on a Windows OS by following these steps:

1. Install [SQL Server Express or Developer](https://www.microsoft.com/sql-server/sql-server-downloads) on your local Windows machine, if it's not already installed.
1. Open the `UpgradeTests.cs` file and delete the `Skip` property in the `[Theory]` attribute on the `ValidateUpgradedOrchestrations` test method.
1. Run the `ValidateUpgradedOrchestrations` test manually in Visual Studio or using `dotnet test`. The test should pass.

In a future update, we will add support for running this test in CI using Docker containers.
