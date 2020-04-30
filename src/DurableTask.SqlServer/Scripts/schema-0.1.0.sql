-- PERSISTENT SCHEMA OBJECTS (tables, indexes, types, etc.)
--
-- The contents of this file must never be changed after
-- being published. Any schema changes must be done in
-- new schema-{major}.{minor}.{patch}.sql scripts.

-- All objects must be created under the "dt" schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dt')
    EXEC('CREATE SCHEMA dt');

-- Create custom types
-- WARNING: Reordering fields is a breaking change!
IF TYPE_ID(N'dt.TaskEvents') IS NULL
    CREATE TYPE dt.TaskEvents AS TABLE (
        SequenceNumber bigint NULL,
        VisibleTime datetime2 NULL,
        InstanceID nvarchar(100) NULL,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NULL,
        [Name] nvarchar(256) NULL,
        RuntimeStatus varchar(30) NULL,
        TaskID int NULL,
        Reason nvarchar(max) NULL,
        PayloadText nvarchar(max) NULL,
        CustomStatusText nvarchar(max) NULL,
        IsPlayed bit NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
        CompletedTime datetime2 NULL
    )
GO

-- Create tables

IF OBJECT_ID(N'dt.Versions', 'U') IS NULL
    CREATE TABLE dt.Versions (
        SemanticVersion nvarchar(100) NOT NULL CONSTRAINT PK_Versions_SemanticVersion PRIMARY KEY WITH (IGNORE_DUP_KEY = ON),
        UpgradeTime datetime2 NOT NULL CONSTRAINT DF_Versions_UpgradeTime DEFAULT SYSUTCDATETIME()
    )


IF OBJECT_ID(N'dt.Instances', 'U') IS NULL
	CREATE TABLE dt.Instances (
		ID nvarchar(100) NOT NULL CONSTRAINT PK_Instances PRIMARY KEY,
		ExecutionID nvarchar(100) NOT NULL,
        [Name] nvarchar(256) NOT NULL,
		CreatedTime datetime2 NOT NULL CONSTRAINT DF_Instances_CreatedTime DEFAULT SYSUTCDATETIME(),
		LastUpdatedTime datetime2 NULL,
        CompletedTime datetime2 NULL,
		RuntimeStatus varchar(20) NOT NULL INDEX IX_Instances_RuntimeStatus NONCLUSTERED,
		CustomStatusText nvarchar(max) NULL,
		InputText nvarchar(max) NULL,
		OutputText nvarchar(max) NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
	)

IF OBJECT_ID(N'dt.NewEvents', 'U') IS NULL
BEGIN
    CREATE TABLE dt.NewEvents (
        -- Metadata columns
        SequenceNumber bigint IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewEvents_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_NewEvents_DequeueCount DEFAULT 0,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_NewEvents_Instances_ID FOREIGN KEY REFERENCES dt.Instances(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        RuntimeStatus varchar(30) NULL,
        TaskID int NULL,
        Reason nvarchar(max) NULL,
        PayloadText nvarchar(max) NULL,
    )

    CREATE CLUSTERED INDEX IX_NewEvents_InstanceId_SequenceNumber ON dt.NewEvents (InstanceID, SequenceNumber)
END

IF OBJECT_ID(N'dt.NewTasks', 'U') IS NULL
    CREATE TABLE dt.NewTasks (
        -- Metadata columns
        SequenceNumber bigint CONSTRAINT PK_NewTasks_SequenceNumber PRIMARY KEY IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewTasks_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_NewTasks_DequeueCount DEFAULT 0,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_NewTasks_Instances_ID FOREIGN KEY REFERENCES dt.Instances(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        TaskID int NOT NULL,
        PayloadText nvarchar(max) NULL,
    )

IF OBJECT_ID(N'dt.History', 'U') IS NULL
    CREATE TABLE dt.History (
	    InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_History_Instances_ID FOREIGN KEY REFERENCES dt.Instances(ID) ON DELETE CASCADE,
	    ExecutionID nvarchar(100) NOT NULL,
        SequenceNumber bigint NOT NULL,
	    EventType varchar(30) NOT NULL,
	    TaskID int NULL,
	    IsPlayed bit NOT NULL CONSTRAINT DF_History_IsPlayed DEFAULT 0,
	    [Timestamp] datetime2 NOT NULL CONSTRAINT DF_History_Timestamp DEFAULT SYSUTCDATETIME(),
	    [Name] nvarchar(256) NULL,
	    PayloadText nvarchar(max) NULL,
	    RuntimeStatus varchar(20) NULL,
        VisibleTime datetime2 NULL,
        CONSTRAINT PK_History_InstanceID_ExecutionID_SequenceNumber PRIMARY KEY (InstanceID, ExecutionID, SequenceNumber)
    )
GO
