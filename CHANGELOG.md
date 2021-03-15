# Changelog

## v0.6.0-alpha

### New

* Support for sub-orchestrations ([#7](https://github.com/microsoft/durabletask-mssql/pull/7)) - contributed by [@usemam](https://github.com/usemam)
* Support for explicit task hub name configuration
* Added `dt.GlobalSettings` table and `dt.SetGlobalSetting` stored procedure
* Added new permissions.sql setup script for setting up databaes permissions
* Added task hub documentation page

## Breaking changes

* Renamed `SqlProviderOptions` to `SqlOrchestrationServiceSettings` and added required constructor parameters
* User-based multitenancy is now disabled by default
* The `dt_runtime` role is now granted access to only specific stored procedures rather than all of them

## v0.5.0-alpha

### New

* Added support for .NET Standard 2.0 (DTFx only) ([#6](https://github.com/microsoft/durabletask-mssql/pull/6))
* Made batch size configurable ([#5](https://github.com/microsoft/durabletask-mssql/pull/5)) - contributed by [@usemam](https://github.com/usemam)

### Improved

* Fixes required for Azure Functions extension bundle compatibility

### Breaking changes

None
