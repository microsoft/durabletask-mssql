# Changelog

## v1.0.1

### Updates

* Fixed script for case sensitive databases ([#113](https://github.com/microsoft/durabletask-mssql/pull/113)) - contributed by [@matei-dorian](https://github.com/matei-dorian)
* Updating SqlDurabilityProviderStartup to implement IWebJobsStartup ([#117](https://github.com/microsoft/durabletask-mssql/pull/117))
* Removed [Microsoft.Azure.Functions.Extensions](https://www.nuget.org/packages/Microsoft.Azure.Functions.Extensions/) package dependency

## v1.0.0

### New

* Support for orchestration rewind ([#96](https://github.com/microsoft/durabletask-mssql/pull/96)) - contributed by [@Greybird](https://github.com/Greybird)
* Added PowerShell script for automated performance testing in Azure
* Added new *LongHaul* stress test to the performance testing project

### Updates

* Updated package version to v1.0.0
* Renamed schema-0.2.0.sql to schema-1.0.0.sql
* Updated [Microsoft.Azure.WebJobs.Extensions.DurableTask](https://www.nuget.org/packages/Microsoft.Azure.WebJobs.Extensions.DurableTask) dependency to v2.7.*
* Fixed data leak in ContinueAsNew path ([#102](https://github.com/microsoft/durabletask-mssql/pull/102))
* Fixed inaccurate license headers
* Fixed "V2 Functions host fails to load when referencing SQL package" ([#107](https://github.com/microsoft/durabletask-mssql/issues/107))

### Breaking changes

* Removed explicit dependencies on `Microsoft.Extensions.Caching.Memory` and `Microsoft.Extensions.Logging.Abstractions`

## v1.0.0-rc2

### New

* Support for purging data with filters ([#80](https://github.com/microsoft/durabletask-mssql/pull/80)) - contributed by [@usemam](https://github.com/usemam)
* Support for new multi-instance query interface ([#88](https://github.com/microsoft/durabletask-mssql/pull/80)) - contributed by [@usemam](https://github.com/usemam)

### Updates

* Removed unnecessary .NET Standard 2.1 target ([#82](https://github.com/microsoft/durabletask-mssql/pull/82))
* Fixed problem terminating orchestration with running activity ([#83](https://github.com/microsoft/durabletask-mssql/pull/83))
* Fixed payload data leak for completed activities (same PR as above)
* Fixed NewEvents leak for completed or continued-as-new instances ([#97](https://github.com/microsoft/durabletask-mssql/pull/97))
* Activity payload IDs are now consistently saved to the history table ([#90](https://github.com/microsoft/durabletask-mssql/issues/90))
* Remove Microsoft.SqlServer.SqlManagementObjects dependency ([#92](https://github.com/microsoft/durabletask-mssql/pull/92)) - contributed by [@IGx89](https://github.com/IGx89)

### Breaking changes

None

## v1.0.0-rc

### Updates

* Support for restarting orchestrations ([#75](https://github.com/microsoft/durabletask-mssql/pull/75))
* Populate ParentInstance in GetOrchestrationStateAsync ([#74](https://github.com/microsoft/durabletask-mssql/pull/74)) - contributed by [@hsnsalhi](https://github.com/hsnsalhi)
* Fix datediff overflow issue ([#70](https://github.com/microsoft/durabletask-mssql/pull/75))
* Added SBOM manifest generation for official nuget packages

### Breaking changes

* Replaced `SqlException` with `InvalidOperationException` for new instance creation

## v0.11.1-beta

### Updates

* Added .NET Standard 2.0 support to Microsoft.DurableTask.SqlServer.AzureFunctions ([#63](https://github.com/microsoft/durabletask-mssql/pull/63))

## v0.11.0-beta

### New

* Feature to automatically create database if not present ([#49](https://github.com/microsoft/durabletask-mssql/pull/49)) - contributed by [@wsugarman](https://github.com/wsugarman)

### Updates

* Fix logic.sql's invalid column usage ([#52](https://github.com/microsoft/durabletask-mssql/pull/52)) - contributed by [@jasonwun](https://github.com/jasonwun)
* Fix for Timer events fire before completion ([#50](https://github.com/microsoft/durabletask-mssql/issues/50)) - thanks to [@vivogaet](https://github.com/vivogaet) for pointing out the issue and the fix.
* Updated performance testing app to Functions v4 and .NET 6
* Updated [Microsoft.Azure.DurableTask.Core](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Core) dependency to v2.6.*.
* Updated [Microsoft.Azure.WebJobs.Extensions.DurableTask](https://www.nuget.org/packages/Microsoft.Azure.WebJobs.Extensions.DurableTask) dependency to v2.6.*.

## v0.10.1-beta

### Updates

* Removed foreign key constraints from all tables to dramatically improve performance and eliminate common sources of deadlocks ([#46](https://github.com/microsoft/durabletask-mssql/pull/46))
* Added documentation for how to work around native dependency issues in Azure Functions.
* Added documentation about the taskEventLockTimeout setting in the Azure Functions host.json file.

## v0.10.0-beta

### Updates

* Updated [Microsoft.Data.SqlClient](https://www.nuget.org/packages/Microsoft.Data.SqlClient) to v3.0.0 - contributed by [@usemam](https://github.com/usemam)

### Breaking changes

* Removed manual Azure Managed Identity configuration (it's configured in the MSSQL connection string now) - contributed by [@usemam](https://github.com/usemam)
* Fixed backwards purge history threshold check ([#39](https://github.com/microsoft/durabletask-mssql/pull/39)) - contributed by [@Jaah](https://github.com/Jaah)

## v0.9.1-beta

### New

* Added extension method for Azure Functions service registration ([#31](https://github.com/microsoft/durabletask-mssql/pull/31))

### Updates

* Updated [Microsoft.Azure.WebJobs.Extensions.DurableTask](https://www.nuget.org/packages/Microsoft.Azure.WebJobs.Extensions.DurableTask) dependency to [v2.5.0](https://github.com/Azure/azure-functions-durable-extension/releases/tag/v2.5.0).
* Updated [Microsoft.Azure.DurableTask.Core](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Core) dependency to [v2.5.5](https://github.com/Azure/durabletask/releases/tag/durabletask.core-v2.5.5).

## v0.9.0-beta

### New

* Support for Azure Managed Identities ([#25](https://github.com/microsoft/durabletask-mssql/pull/25)) - contributed by [@usemam](https://github.com/usemam)

## v0.8.0-beta

### New

* Support for multi-instance queries in Azure Functions ([#21](https://github.com/microsoft/durabletask-mssql/pull/21))
* Instance purge, long timers, and internal Linux telemetry support in Azure Functions ([#22](https://github.com/microsoft/durabletask-mssql/pull/22))

### Updates

* Fix deadlock on activity message deletion ([#20](https://github.com/microsoft/durabletask-mssql/pull/20))
* Updated [Microsoft.Azure.WebJobs.Extensions.DurableTask](https://www.nuget.org/packages/Microsoft.Azure.WebJobs.Extensions.DurableTask) dependency to [v2.4.3](https://github.com/Azure/azure-functions-durable-extension/releases/tag/v2.4.3).

### Breaking changes

* Renamed `dt.PurgeInstanceState` SQL stored procedure to `dt.PurgeInstanceStateByTime`

## v0.7.0-alpha

### New

* Added `dt.GetScaleMetric` SQL function for use with the [MSSQL KEDA Scaler](https://keda.sh/docs/scalers/mssql/).
* Added `dt.GetScaleRecommendation` SQL function and `IScaleProvider` implementation for VNET scaling in Azure Functions.
* Added versioning support for task activities ([#14](https://github.com/microsoft/durabletask-mssql/pull/14)) - contributed by [@usemam](https://github.com/usemam)

### Updates

* Switched default task hub mode back to multitenant to simplify testing
* Updated [Microsoft.Azure.WebJobs.Extensions.DurableTask](https://www.nuget.org/packages/Microsoft.Azure.WebJobs.Extensions.DurableTask) dependency to [v2.4.2](https://github.com/Azure/azure-functions-durable-extension/releases/tag/v2.4.2).

### Breaking changes

* Changed `SqlDurabilityProviderFactory` and `SqlDurabilityOptions` classes from `public` to `internal`.

## v0.6.0-alpha

### New

* Support for sub-orchestrations ([#7](https://github.com/microsoft/durabletask-mssql/pull/7)) - contributed by [@usemam](https://github.com/usemam)
* Support for explicit task hub name configuration ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))
* Added `dt.GlobalSettings` table and `dt.SetGlobalSetting` stored procedure ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))
* Added new permissions.sql setup script for setting up databaes permissions ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))
* Added task hub documentation page ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))

### Breaking changes

* Renamed `SqlProviderOptions` to `SqlOrchestrationServiceSettings` and added required constructor parameters ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))
* User-based multitenancy is now disabled by default ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))
* The `dt_runtime` role is now granted access to only specific stored procedures rather than all of them ([#10](https://github.com/microsoft/durabletask-mssql/pull/10))

## v0.5.0-alpha

### New

* Added support for .NET Standard 2.0 (DTFx only) ([#6](https://github.com/microsoft/durabletask-mssql/pull/6))
* Made batch size configurable ([#5](https://github.com/microsoft/durabletask-mssql/pull/5)) - contributed by [@usemam](https://github.com/usemam)

### Improved

* Fixes required for Azure Functions extension bundle compatibility

### Breaking changes

None
