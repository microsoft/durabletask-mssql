-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- Add a new TraceContext column to the Instances table
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.Instances') AND name = 'TraceContext')
    ALTER TABLE __SchemaNamePlaceholder__.Instances ADD TraceContext varchar(800) NULL

-- Add a new TraceContext column to the History table
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.History') AND name = 'TraceContext')
    ALTER TABLE __SchemaNamePlaceholder__.History ADD TraceContext varchar(800) NULL

-- Add a new TraceContext column to the NewEvents table
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.NewEvents') AND name = 'TraceContext')
    ALTER TABLE __SchemaNamePlaceholder__.NewEvents ADD TraceContext varchar(800) NULL

-- Add a new TraceContext column to the NewTasks table
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.NewTasks') AND name = 'TraceContext')
    ALTER TABLE __SchemaNamePlaceholder__.NewTasks ADD TraceContext varchar(800) NULL

-- Drop custom types that have schema changes. They will be recreated in logic.sql, which executes last.
-- In this release, we have changes to HistoryEvents, OrchestrationEvents, and TaskEvents to add TraceContext fields.
-- In order to drop the types, we must first drop all stored procedures that depend on them, and then drop the types.
-- One way to discover all the stored procs that depend on the types is to query sys.sql_expression_dependencies
-- (credit to https://www.mssqltips.com/sqlservertip/6114/how-to-alter-user-defined-table-type-in-sql-server/):

/*
    SELECT DISTINCT [types].name FROM (
	    SELECT s.name as [schema], o.name, def = OBJECT_DEFINITION(d.referencing_id), d.referenced_entity_name
	      FROM sys.sql_expression_dependencies AS d
	      INNER JOIN sys.objects AS o
		     ON d.referencing_id = o.[object_id]
	      INNER JOIN sys.schemas AS s
		     ON o.[schema_id] = s.[schema_id]
	      WHERE d.referenced_database_name IS NULL
		    AND d.referenced_class_desc = 'TYPE'
            AND d.referenced_entity_name IN ('HistoryEvents', 'OrchestrationEvents', 'TaskEvents')
    ) [types]
*/

-- First, drop the rferencing stored procedures
IF OBJECT_ID('__SchemaNamePlaceholder__._AddOrchestrationEvents') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._AddOrchestrationEvents
IF OBJECT_ID('__SchemaNamePlaceholder__._CheckpointOrchestration') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CheckpointOrchestration
IF OBJECT_ID('__SchemaNamePlaceholder__._CompleteTasks') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CompleteTasks

-- Next, drop the types that we are changing
IF TYPE_ID('__SchemaNamePlaceholder__.HistoryEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.HistoryEvents
IF TYPE_ID('__SchemaNamePlaceholder__.OrchestrationEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.OrchestrationEvents
IF TYPE_ID('__SchemaNamePlaceholder__.TaskEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.TaskEvents
