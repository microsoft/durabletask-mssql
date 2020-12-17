-- Functions
DROP FUNCTION IF EXISTS dt.CurrentTaskHub

-- Views
DROP VIEW IF EXISTS dt.vHistory
DROP VIEW IF EXISTS dt.vInstances

-- Public Sprocs
DROP PROCEDURE IF EXISTS dt.CreateInstance
DROP PROCEDURE IF EXISTS dt.QuerySingleOrchestration
DROP PROCEDURE IF EXISTS dt.RaiseEvent
DROP PROCEDURE IF EXISTS dt.TerminateInstance
DROP PROCEDURE IF EXISTS dt.PurgeInstanceState

-- Private sprocs
DROP PROCEDURE IF EXISTS dt._AddOrchestrationEvents
DROP PROCEDURE IF EXISTS dt._CheckpointOrchestration
DROP PROCEDURE IF EXISTS dt._CompleteTasks
DROP PROCEDURE IF EXISTS dt._GetVersions
DROP PROCEDURE IF EXISTS dt._LockNextOrchestration
DROP PROCEDURE IF EXISTS dt._LockNextTask
DROP PROCEDURE IF EXISTS dt._RenewOrchestrationLocks
DROP PROCEDURE IF EXISTS dt._RenewTaskLocks
DROP PROCEDURE IF EXISTS dt._UpdateVersion

-- Tables
DROP TABLE IF EXISTS dt.Versions
DROP TABLE IF EXISTS dt.NewTasks
DROP TABLE IF EXISTS dt.NewEvents
DROP TABLE IF EXISTS dt.History
DROP TABLE IF EXISTS dt.Instances
DROP TABLE IF EXISTS dt.Payloads

-- Custom types
DROP TYPE IF EXISTS dt.MessageIDs
DROP TYPE IF EXISTS dt.HistoryEvents
DROP TYPE IF EXISTS dt.OrchestrationEvents
DROP TYPE IF EXISTS dt.TaskEvents

-- This must be the last DROP statement related to schema
DROP SCHEMA IF EXISTS dt

-- Roles: all members have to be dropped before the role can be dropped
DECLARE @rolename sysname = 'dt_runtime';
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

DROP ROLE IF EXISTS dt_runtime
