-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- Add a new Tags column to the Instances table (JSON blob of string key-value pairs).
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.Instances') AND name = 'Tags')
    ALTER TABLE __SchemaNamePlaceholder__.Instances ADD [Tags] varchar(8000) NULL

-- Add a new Tags column to the NewTasks table so that orchestration tags
-- propagate to activity task workers via OrchestrationExecutionContext.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.NewTasks') AND name = 'Tags')
    ALTER TABLE __SchemaNamePlaceholder__.NewTasks ADD [Tags] varchar(8000) NULL

-- Add Tags columns to the OrchestrationEvents and TaskEvents table types.
-- To change a type we must first drop all stored procedures that reference it,
-- then drop the type itself. The types and sprocs will be recreated by logic.sql
-- which executes afterwards.
IF OBJECT_ID('__SchemaNamePlaceholder__._AddOrchestrationEvents') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._AddOrchestrationEvents
IF OBJECT_ID('__SchemaNamePlaceholder__._CheckpointOrchestration') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CheckpointOrchestration
IF OBJECT_ID('__SchemaNamePlaceholder__._CompleteTasks') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CompleteTasks

IF TYPE_ID('__SchemaNamePlaceholder__.OrchestrationEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.OrchestrationEvents
IF TYPE_ID('__SchemaNamePlaceholder__.TaskEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.TaskEvents
