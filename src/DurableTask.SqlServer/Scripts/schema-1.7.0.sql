-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- Add a covering index on NewTasks to improve the performance of _LockNextTask.
-- This allows SQL Server to efficiently find unlocked, visible tasks without
-- performing a full clustered-index scan, which is important under high concurrency
-- when there may be a backlog of locked or not-yet-visible tasks.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('__SchemaNamePlaceholder__.NewTasks') AND name = 'IX_NewTasks_LockNext')
    CREATE NONCLUSTERED INDEX IX_NewTasks_LockNext ON __SchemaNamePlaceholder__.NewTasks(TaskHub, LockExpiration, VisibleTime)
        INCLUDE (SequenceNumber, LockedBy, DequeueCount)
GO
