-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- Functions
DROP FUNCTION IF EXISTS {{SchemaNamePlaceholder}}.CurrentTaskHub
DROP FUNCTION IF EXISTS {{SchemaNamePlaceholder}}.GetScaleMetric
DROP FUNCTION IF EXISTS {{SchemaNamePlaceholder}}.GetScaleRecommendation

-- Views
DROP VIEW IF EXISTS {{SchemaNamePlaceholder}}.vHistory
DROP VIEW IF EXISTS {{SchemaNamePlaceholder}}.vInstances

-- Public Sprocs
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.CreateInstance
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.GetInstanceHistory
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.QuerySingleOrchestration
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.RaiseEvent
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.SetGlobalSetting
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.TerminateInstance
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.PurgeInstanceStateByID
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}.PurgeInstanceStateByTime

-- Private sprocs
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._AddOrchestrationEvents
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._CheckpointOrchestration
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._CompleteTasks
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._DiscardEventsAndUnlockInstance
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._GetVersions
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._LockNextOrchestration
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._LockNextTask
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._QueryManyOrchestrations
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._RenewOrchestrationLocks
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._RenewTaskLocks
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._UpdateVersion
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._RewindInstance
DROP PROCEDURE IF EXISTS {{SchemaNamePlaceholder}}._RewindInstanceRecursive

-- Tables
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.Versions
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.NewTasks
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.NewEvents
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.History
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.Instances
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.Payloads
DROP TABLE IF EXISTS {{SchemaNamePlaceholder}}.GlobalSettings

-- Custom types
DROP TYPE IF EXISTS {{SchemaNamePlaceholder}}.HistoryEvents
DROP TYPE IF EXISTS {{SchemaNamePlaceholder}}.InstanceIDs
DROP TYPE IF EXISTS {{SchemaNamePlaceholder}}.MessageIDs
DROP TYPE IF EXISTS {{SchemaNamePlaceholder}}.OrchestrationEvents
DROP TYPE IF EXISTS {{SchemaNamePlaceholder}}.TaskEvents

-- This must be the last DROP statement related to schema
DROP SCHEMA IF EXISTS {{SchemaNamePlaceholder}}

-- Roles: all members have to be dropped before the role can be dropped
DECLARE @rolename sysname = '{{SchemaNamePlaceholder}}_runtime';
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

DROP ROLE IF EXISTS {{SchemaNamePlaceholder}}_runtime
