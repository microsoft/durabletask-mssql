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
IF TYPE_ID('__SchemaNamePlaceholder__.HistoryEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.HistoryEvents
IF TYPE_ID('__SchemaNamePlaceholder__.OrchestrationEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.OrchestrationEvents
IF TYPE_ID('__SchemaNamePlaceholder__.TaskEvents') IS NOT NULL
    DROP TYPE __SchemaNamePlaceholder__.TaskEvents
