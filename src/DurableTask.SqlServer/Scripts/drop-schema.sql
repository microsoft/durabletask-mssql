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

-- Private sprocs
DROP PROCEDURE IF EXISTS dt._AddOrchestrationEvents
DROP PROCEDURE IF EXISTS dt._CheckpointOrchestration
DROP PROCEDURE IF EXISTS dt._CompleteTasks
DROP PROCEDURE IF EXISTS dt._GetVersions
DROP PROCEDURE IF EXISTS dt._LockNextOrchestration
DROP PROCEDURE IF EXISTS dt._LockNextTask
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

-- This must be the last DROP statement
DROP SCHEMA IF EXISTS dt
