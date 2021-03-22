# Changelog

## v0.7.0-alpha

### New

* Added `dt.GetScaleMetric` SQL function for use with the [MSSQL KEDA Scaler](https://keda.sh/docs/scalers/mssql/).

### Updates

* Switched default task hub mode back to multitenant, since it simplifies certain test setups

### Breaking changes

* None

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
