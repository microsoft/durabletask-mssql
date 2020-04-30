-- DROP ALL SCHEMA (tables, views, sprocs, types, etc.)

-- This script should remove all "dt" objects from the target database 
-- (but should not delete the database itself). It MUST be possible to
-- run this script multiple times in succession and still succeed. Think
-- of it as the "uninstall" script. Note that order matters.

-- Drop stored procedures
DROP PROCEDURE IF EXISTS dt.LockNextOrchestration
DROP PROCEDURE IF EXISTS dt.CheckpointOrchestration
DROP PROCEDURE IF EXISTS dt.CreateInstances
DROP PROCEDURE IF EXISTS dt.QuerySingleOrchestration
DROP PROCEDURE IF EXISTS dt.CompleteTasks
DROP PROCEDURE IF EXISTS dt.LockNextTask
DROP PROCEDURE IF EXISTS dt.GetVersions
DROP PROCEDURE IF EXISTS dt.UpdateVersion

-- Tables
DROP TABLE IF EXISTS dt.NewEvents
DROP TABLE IF EXISTS dt.NewTasks
DROP TABLE IF EXISTS dt.History
DROP TABLE IF EXISTS dt.Instances
DROP TABLE IF EXISTS dt.Versions

-- Custom types
DROP TYPE IF EXISTS dt.TaskEvents

-- This must be the last DROP statement
DROP SCHEMA IF EXISTS dt
