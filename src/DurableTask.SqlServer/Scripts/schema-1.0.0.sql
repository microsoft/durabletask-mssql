-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- All objects must be created under the "dt" schema or under a custom schema.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '__SchemaNamePlaceholder__')
    EXEC('CREATE SCHEMA __SchemaNamePlaceholder__');

-- Create custom types
IF TYPE_ID(N'__SchemaNamePlaceholder__.InstanceIDs') IS NULL
    CREATE TYPE __SchemaNamePlaceholder__.InstanceIDs AS TABLE (
        [InstanceID] varchar(100) NOT NULL
    )
GO

IF TYPE_ID(N'__SchemaNamePlaceholder__.MessageIDs') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE __SchemaNamePlaceholder__.MessageIDs AS TABLE (
        [InstanceID] varchar(100) NULL,
        [SequenceNumber] bigint NULL
    )
GO

IF TYPE_ID(N'__SchemaNamePlaceholder__.HistoryEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE __SchemaNamePlaceholder__.HistoryEvents AS TABLE (
        [InstanceID] varchar(100) NULL,
        [ExecutionID] varchar(50) NULL,
        [SequenceNumber] bigint NULL,
        [EventType] varchar(40) NULL,
        [Name] varchar(300) NULL,
        [RuntimeStatus] varchar(30) NULL,
        [TaskID] int NULL,
        [Timestamp] datetime2 NULL,
        [IsPlayed] bit NULL,
        [VisibleTime] datetime2 NULL,
        [Reason] varchar(max) NULL,
        [PayloadText] varchar(max) NULL,
        [PayloadID] uniqueidentifier NULL,
        [ParentInstanceID] varchar(100) NULL,
        [Version] varchar(100) NULL
    )
GO

IF TYPE_ID(N'__SchemaNamePlaceholder__.OrchestrationEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE __SchemaNamePlaceholder__.OrchestrationEvents AS TABLE (
        [InstanceID] varchar(100) NULL,
        [ExecutionID] varchar(50) NULL,
        [SequenceNumber] bigint NULL,
        [EventType] varchar(40) NULL,
        [Name] varchar(300) NULL,
        [RuntimeStatus] varchar(30) NULL,
        [TaskID] int NULL,
        [VisibleTime] datetime2 NULL,
        [Reason] varchar(max) NULL,
        [PayloadText] varchar(max) NULL,
        [PayloadID] uniqueidentifier NULL,
        [ParentInstanceID] varchar(100) NULL,
        [Version] varchar(100) NULL
    )
GO

IF TYPE_ID(N'__SchemaNamePlaceholder__.TaskEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE __SchemaNamePlaceholder__.TaskEvents AS TABLE (
        [InstanceID] varchar(100) NULL,
        [ExecutionID] varchar(50) NULL,
        [Name] varchar(300) NULL,
        [EventType] varchar(40) NULL,
        [TaskID] int NULL,
        [VisibleTime] datetime2 NULL,
        [LockedBy] varchar(100) NULL,
        [LockExpiration] datetime2 NULL,
        [Reason] varchar(max) NULL,
        [PayloadText] varchar(max) NULL,
        [PayloadID] uniqueidentifier NULL,
        [Version] varchar(100) NULL
    )
GO

-- Create tables

-- Rule #1: Use varchar instead of nvarchar
-- Rule #2: Do not use varchar(MAX) except in the Payloads table
-- Rule #3: Try to follow existing naming and ordering conventions

IF OBJECT_ID(N'__SchemaNamePlaceholder__.Versions', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.Versions (
        SemanticVersion varchar(100) NOT NULL CONSTRAINT PK_Versions_SemanticVersion PRIMARY KEY WITH (IGNORE_DUP_KEY = ON),
        UpgradeTime datetime2 NOT NULL CONSTRAINT DF_Versions_UpgradeTime DEFAULT SYSUTCDATETIME()
    )
END
GO

IF OBJECT_ID(N'__SchemaNamePlaceholder__.Payloads', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.Payloads (
        [TaskHub] varchar(50) NOT NULL,
        [InstanceID] varchar(100) NOT NULL,
        [PayloadID] uniqueidentifier NOT NULL,
        [Text] varchar(max) NULL,
        [Reason] varchar(max) NULL,
        -- NOTE: no FK constraint to Instances table because we want to allow events to create new instances

        CONSTRAINT PK_Payloads PRIMARY KEY (TaskHub, InstanceID, PayloadID),
    )
END
GO

IF OBJECT_ID(N'__SchemaNamePlaceholder__.Instances', 'U') IS NULL
BEGIN
	CREATE TABLE __SchemaNamePlaceholder__.Instances (
		[TaskHub] varchar(50) NOT NULL,
        [InstanceID] varchar(100) NOT NULL,
		[ExecutionID] varchar(50) NOT NULL CONSTRAINT DF_Instances_ExecutionID DEFAULT (NEWID()), -- expected to be system generated
        [Name] varchar(300) NOT NULL, -- the type name of the orchestration or entity
        [Version] varchar(100) NULL, -- the version of the orchestration (optional)
		[CreatedTime] datetime2 NOT NULL CONSTRAINT DF_Instances_CreatedTime DEFAULT SYSUTCDATETIME(),
		[LastUpdatedTime] datetime2 NULL,
        [CompletedTime] datetime2 NULL,
		[RuntimeStatus] varchar(20) NOT NULL,
        [LockedBy] varchar(100) NULL,
        [LockExpiration] datetime2 NULL,
		[InputPayloadID] uniqueidentifier NULL,
		[OutputPayloadID] uniqueidentifier NULL,
		[CustomStatusPayloadID] uniqueidentifier NULL,
        [ParentInstanceID] varchar(100) NULL,

        CONSTRAINT PK_Instances PRIMARY KEY (TaskHub, InstanceID),
        -- NOTE: No FK constraints for the Payloads table because of high performance cost and deadlock risk
	)

    -- This index is used by LockNext and Purge logic
    CREATE INDEX IX_Instances_RuntimeStatus ON __SchemaNamePlaceholder__.Instances(TaskHub, RuntimeStatus)
        INCLUDE ([LockExpiration], [CreatedTime], [CompletedTime])
    
    -- This index is intended to help the performance of multi-instance query
    CREATE INDEX IX_Instances_CreatedTime ON __SchemaNamePlaceholder__.Instances(TaskHub, CreatedTime)
        INCLUDE ([RuntimeStatus], [CompletedTime], [InstanceID])
END

IF OBJECT_ID(N'__SchemaNamePlaceholder__.NewEvents', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.NewEvents (
        [SequenceNumber] bigint IDENTITY NOT NULL, -- order is important for FIFO
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewEvents_Timestamp DEFAULT SYSUTCDATETIME(),
        [VisibleTime] datetime2 NULL, -- for scheduled messages
        [DequeueCount] int NOT NULL CONSTRAINT DF_NewEvents_DequeueCount DEFAULT 0,
		[TaskHub] varchar(50) NOT NULL,
        [InstanceID] varchar(100) NOT NULL,
        [ExecutionID] varchar(50) NULL,
        [EventType] varchar(40) NOT NULL,
        [RuntimeStatus] varchar(30) NULL,
        [Name] varchar(300) NULL,
        [TaskID] int NULL,
        [PayloadID] uniqueidentifier NULL,

        CONSTRAINT PK_NewEvents PRIMARY KEY (TaskHub, InstanceID, SequenceNumber),
        -- NOTE: no FK constraint to Instances and Payloads tables because of high performance cost and deadlock risk.
        --       Also, we want to allow events to create new instances, which means an Instances row might not yet exist.
    )
END

IF OBJECT_ID(N'__SchemaNamePlaceholder__.History', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.History (
        [TaskHub] varchar(50) NOT NULL,
        [InstanceID] varchar(100) NOT NULL,
	    [ExecutionID] varchar(50) NOT NULL,
        [SequenceNumber] bigint NOT NULL,
	    [EventType] varchar(40) NOT NULL,
	    [TaskID] int NULL,
	    [Timestamp] datetime2 NOT NULL CONSTRAINT DF_History_Timestamp DEFAULT SYSUTCDATETIME(),
	    [IsPlayed] bit NOT NULL CONSTRAINT DF_History_IsPlayed DEFAULT 0,
	    [Name] varchar(300) NULL,
	    [RuntimeStatus] varchar(20) NULL,
        [VisibleTime] datetime2 NULL,
	    [DataPayloadID] uniqueidentifier NULL,

        CONSTRAINT PK_History PRIMARY KEY (TaskHub, InstanceID, ExecutionID, SequenceNumber),
        -- NOTE: no FK constraint to Payloads or Instances tables because of high performance cost and deadlock risk
    )
END

IF OBJECT_ID(N'__SchemaNamePlaceholder__.NewTasks', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.NewTasks (
        [TaskHub] varchar(50) NOT NULL,
        [SequenceNumber] bigint IDENTITY NOT NULL,  -- order is important for FIFO
        [InstanceID] varchar(100) NOT NULL,
        [ExecutionID] varchar(50) NULL,
        [Name] varchar(300) NULL,
        [TaskID] int NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewTasks_Timestamp DEFAULT SYSUTCDATETIME(),
        [VisibleTime] datetime2 NULL,
        [DequeueCount] int NOT NULL CONSTRAINT DF_NewTasks_DequeueCount DEFAULT 0,
        [LockedBy] varchar(100) NULL,
        [LockExpiration] datetime2 NULL,
        [PayloadID] uniqueidentifier NULL,
        [Version] varchar(100) NULL,

        CONSTRAINT PK_NewTasks PRIMARY KEY (TaskHub, SequenceNumber),
        -- NOTE: no FK constraint to Payloads or Instances tables because of high performance cost and deadlock risk
    )

    -- This index is used by vScaleHints
    CREATE NONCLUSTERED INDEX IX_NewTasks_InstanceID ON __SchemaNamePlaceholder__.NewTasks(TaskHub, InstanceID)
        INCLUDE ([SequenceNumber], [Timestamp], [LockExpiration], [VisibleTime])
END
GO

IF OBJECT_ID(N'__SchemaNamePlaceholder__.GlobalSettings', 'U') IS NULL
BEGIN
    CREATE TABLE __SchemaNamePlaceholder__.GlobalSettings (
        [Name] varchar(300) NOT NULL PRIMARY KEY,
        [Value] sql_variant NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_GlobalSettings_Timestamp DEFAULT SYSUTCDATETIME(),
        [LastModifiedBy] nvarchar(128) NOT NULL CONSTRAINT DF_GlobalSettings_LastModifiedby DEFAULT USER_NAME()
    )
    
    -- Default task hub mode is 1, or "User ID"
    INSERT INTO __SchemaNamePlaceholder__.GlobalSettings ([Name], [Value]) VALUES ('TaskHubMode', 1)
END
GO
