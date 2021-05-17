-- Copyright (c) .NET Foundation. All rights reserved.
-- Licensed under the MIT License. See LICENSE in the project root for license information.

-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- All objects must be created under the "dt" schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dt')
    EXEC('CREATE SCHEMA dt');

-- Create custom types
IF TYPE_ID(N'dt.MessageIDs') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE dt.MessageIDs AS TABLE (
        [InstanceID] varchar(100) NULL,
        [SequenceNumber] bigint NULL
    )
GO

IF TYPE_ID(N'dt.HistoryEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE dt.HistoryEvents AS TABLE (
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

IF TYPE_ID(N'dt.OrchestrationEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE dt.OrchestrationEvents AS TABLE (
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

IF TYPE_ID(N'dt.TaskEvents') IS NULL
    -- WARNING: Reordering fields is a breaking change!
    CREATE TYPE dt.TaskEvents AS TABLE (
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
-- Rule #3: Try to follow existing nameing and ordering conventions

IF OBJECT_ID(N'dt.Versions', 'U') IS NULL
BEGIN
    CREATE TABLE dt.Versions (
        SemanticVersion varchar(100) NOT NULL CONSTRAINT PK_Versions_SemanticVersion PRIMARY KEY WITH (IGNORE_DUP_KEY = ON),
        UpgradeTime datetime2 NOT NULL CONSTRAINT DF_Versions_UpgradeTime DEFAULT SYSUTCDATETIME()
    )
END
GO

IF OBJECT_ID(N'dt.Payloads', 'U') IS NULL
BEGIN
    CREATE TABLE dt.Payloads (
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

IF OBJECT_ID(N'dt.Instances', 'U') IS NULL
BEGIN
	CREATE TABLE dt.Instances (
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
        CONSTRAINT FK_Instances_Input_Payloads FOREIGN KEY (TaskHub, InstanceID, InputPayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID),
        CONSTRAINT FK_Instances_Output_Payloads FOREIGN KEY (TaskHub, InstanceID, OutputPayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID),
        CONSTRAINT FK_Instances_CustomStatus_Payloads FOREIGN KEY (TaskHub, InstanceID, CustomStatusPayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID)
	)

    -- This index is used by LockNext and Purge logic
    CREATE INDEX IX_Instances_RuntimeStatus ON dt.Instances(TaskHub, RuntimeStatus)
        INCLUDE ([LockExpiration], [CreatedTime], [CompletedTime])
    
    -- This index is intended to help the performance of multi-instance query
    CREATE INDEX IX_Instances_CreatedTime ON dt.Instances(TaskHub, CreatedTime)
        INCLUDE ([RuntimeStatus], [CompletedTime], [InstanceID])
END

IF OBJECT_ID(N'dt.NewEvents', 'U') IS NULL
BEGIN
    CREATE TABLE dt.NewEvents (
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
        CONSTRAINT FK_NewEvents_Payloads FOREIGN KEY (TaskHub, InstanceID, PayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID)
        -- NOTE: no FK constraint to Instances table because we want to allow events to create new instances
    )
END

IF OBJECT_ID(N'dt.History', 'U') IS NULL
    CREATE TABLE dt.History (
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
        CONSTRAINT FK_History_Instances FOREIGN KEY (TaskHub, InstanceID) REFERENCES dt.Instances(TaskHub, InstanceID) ON DELETE CASCADE,
        CONSTRAINT FK_History_Payloads FOREIGN KEY (TaskHub, InstanceID, DataPayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID)
    )

IF OBJECT_ID(N'dt.NewTasks', 'U') IS NULL
    CREATE TABLE dt.NewTasks (
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
        CONSTRAINT FK_NewTasks_Instances FOREIGN KEY (TaskHub, InstanceID) REFERENCES dt.Instances(TaskHub, InstanceID) ON DELETE CASCADE,
        CONSTRAINT FK_NewTasks_Payloads FOREIGN KEY (TaskHub, InstanceID, PayloadID) REFERENCES dt.Payloads(TaskHub, InstanceID, PayloadID)
    )

    -- This index is used by vScaleHints
    CREATE NONCLUSTERED INDEX IX_NewTasks_InstanceID ON dt.NewTasks(TaskHub, InstanceID)
        INCLUDE ([SequenceNumber], [Timestamp], [LockExpiration], [VisibleTime])
GO

IF OBJECT_ID(N'dt.GlobalSettings', 'U') IS NULL
BEGIN
    CREATE TABLE dt.GlobalSettings (
        [Name] varchar(300) NOT NULL PRIMARY KEY,
        [Value] sql_variant NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_GlobalSettings_Timestamp DEFAULT SYSUTCDATETIME(),
        [LastModifiedBy] nvarchar(128) NOT NULL CONSTRAINT DF_GlobalSettings_LastModifiedby DEFAULT USER_NAME()
    )
    
    -- Default task hub mode is 1, or "User ID"
    INSERT INTO dt.GlobalSettings ([Name], [Value]) VALUES ('TaskHubMode', 1)
END
GO
