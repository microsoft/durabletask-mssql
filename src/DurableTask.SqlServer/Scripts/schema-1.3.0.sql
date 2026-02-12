-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- Add a new Tags column to the Instances table (JSON blob of string key-value pairs)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.Instances') AND name = 'Tags')
    ALTER TABLE __SchemaNamePlaceholder__.Instances ADD [Tags] varchar(MAX) NULL

-- Add a new Tags column to the NewTasks table so that activity middleware can access orchestration tags
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.NewTasks') AND name = 'Tags')
    ALTER TABLE __SchemaNamePlaceholder__.NewTasks ADD [Tags] varchar(MAX) NULL

-- Drop custom types that have schema changes (OrchestrationEvents and TaskEvents get a Tags field).
-- Must first drop all stored procedures that depend on them.
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
            AND d.referenced_entity_name IN ('OrchestrationEvents', 'TaskEvents')
    ) [types]
*/

-- First, drop the referencing stored procedures
IF OBJECT_ID('__SchemaNamePlaceholder__._AddOrchestrationEvents') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._AddOrchestrationEvents
IF OBJECT_ID('__SchemaNamePlaceholder__._CheckpointOrchestration') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CheckpointOrchestration
IF OBJECT_ID('__SchemaNamePlaceholder__._CompleteTasks') IS NOT NULL
    DROP PROCEDURE __SchemaNamePlaceholder__._CompleteTasks

-- Next, drop the types that we are changing
IF TYPE_ID('__SchemaNamePlaceholder__.OrchestrationEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.OrchestrationEvents
IF TYPE_ID('__SchemaNamePlaceholder__.TaskEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.TaskEvents
