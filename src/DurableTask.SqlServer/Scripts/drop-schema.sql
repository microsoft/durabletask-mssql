-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- Functions
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__._CurrentTaskHub
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__.CurrentTaskHub
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__._GetScaleMetric
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__.GetScaleMetric
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__._GetScaleRecommendation
DROP FUNCTION IF EXISTS __SchemaNamePlaceholder__.GetScaleRecommendation

-- Views
DROP VIEW IF EXISTS __SchemaNamePlaceholder__.vHistory
DROP VIEW IF EXISTS __SchemaNamePlaceholder__.vInstances

-- Public Sprocs
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.CreateInstance
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.GetInstanceHistory
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.QuerySingleOrchestration
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.RaiseEvent
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.SetGlobalSetting
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.TerminateInstance
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.PurgeInstanceStateByID
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__.PurgeInstanceStateByTime

-- Private sprocs
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._AddOrchestrationEvents
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._CheckpointOrchestration
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._CompleteTasks
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._DiscardEventsAndUnlockInstance
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._GetVersions
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._LockNextOrchestration
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._LockNextTask
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._QueryManyOrchestrations
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._RenewOrchestrationLocks
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._RenewTaskLocks
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._UpdateVersion
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._RewindInstance
DROP PROCEDURE IF EXISTS __SchemaNamePlaceholder__._RewindInstanceRecursive

-- Tables
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.Versions
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.NewTasks
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.NewEvents
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.History
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.Instances
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.Payloads
DROP TABLE IF EXISTS __SchemaNamePlaceholder__.GlobalSettings

-- Custom types
DROP TYPE IF EXISTS __SchemaNamePlaceholder__.HistoryEvents
DROP TYPE IF EXISTS __SchemaNamePlaceholder__.InstanceIDs
DROP TYPE IF EXISTS __SchemaNamePlaceholder__.MessageIDs
DROP TYPE IF EXISTS __SchemaNamePlaceholder__.OrchestrationEvents
DROP TYPE IF EXISTS __SchemaNamePlaceholder__.TaskEvents

-- This must be the last DROP statement related to schema
DROP SCHEMA IF EXISTS __SchemaNamePlaceholder__

-- Roles: all members have to be dropped before the role can be dropped
DECLARE @rolename sysname = '__SchemaNamePlaceholder___runtime';
DECLARE @cmd AS nvarchar(MAX) = N'';
SELECT @cmd = @cmd + '
    ALTER ROLE ' + QUOTENAME(@rolename) + ' DROP MEMBER ' + QUOTENAME(members.[name]) + ';'
FROM sys.database_role_members AS rolemembers
    JOIN sys.database_principals AS roles 
        ON roles.[principal_id] = rolemembers.[role_principal_id]
    JOIN sys.database_principals AS members 
        ON members.[principal_id] = rolemembers.[member_principal_id]
WHERE roles.[name] = @rolename
EXEC(@cmd);

-- Using EXEC
DROP ROLE IF EXISTS __SchemaNamePlaceholder___runtime
