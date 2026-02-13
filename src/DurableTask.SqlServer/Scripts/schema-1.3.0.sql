-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- Add a new Tags column to the Instances table (JSON blob of string key-value pairs).
-- Tags are read directly from the Instances table in all stored procedures,
-- so no changes to the OrchestrationEvents/TaskEvents types or NewTasks table are needed.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.Instances') AND name = 'Tags')
    ALTER TABLE __SchemaNamePlaceholder__.Instances ADD [Tags] varchar(MAX) NULL
