-- Drop stored procedures
IF OBJECT_ID('LockNextOrchestration') IS NOT NULL
    DROP PROCEDURE LockNextOrchestration

IF OBJECT_ID('AddNewEvents') IS NOT NULL
    DROP PROCEDURE AddNewEvents

IF OBJECT_ID('CheckpointOrchestration') IS NOT NULL
    DROP PROCEDURE CheckpointOrchestration

IF OBJECT_ID('CreateInstances') IS NOT NULL
    DROP PROCEDURE CreateInstances

IF OBJECT_ID('QuerySingleOrchestration') IS NOT NULL
    DROP PROCEDURE QuerySingleOrchestration

IF OBJECT_ID('CompleteTasks') IS NOT NULL
    DROP PROCEDURE CompleteTasks

-- Drop tables
IF OBJECT_ID(N'NewEvents', 'U') IS NOT NULL
    DROP TABLE NewEvents

IF OBJECT_ID(N'NewTasks', 'U') IS NOT NULL
    DROP TABLE NewTasks

IF OBJECT_ID(N'History', 'U') IS NOT NULL
    DROP TABLE History

IF OBJECT_ID(N'Instances', 'U') IS NOT NULL
    DROP TABLE Instances

-- Drop custom types
IF TYPE_ID(N'TaskEvents') IS NOT NULL
    DROP TYPE TaskEvents
GO

-- Create custom types
-- WARNING: Reordering fields is a breaking change!
IF TYPE_ID(N'TaskEvents') IS NULL
    CREATE TYPE TaskEvents AS TABLE (
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
        LockExpiration datetime2 NULL,
        CompletedTime datetime2 NULL
    )
GO

-- Create tables

IF OBJECT_ID(N'dbo.Instances', 'U') IS NULL
	CREATE TABLE Instances (
		ID nvarchar(100) NOT NULL CONSTRAINT PK_Instances PRIMARY KEY,
		ExecutionID nvarchar(100) NOT NULL,
        [Name] nvarchar(256) NOT NULL,
		CreatedTime datetime2 NOT NULL CONSTRAINT DF_Instances_CreatedTime DEFAULT SYSUTCDATETIME(),
		LastUpdatedTime datetime2 NULL,
        CompletedTime datetime2 NULL,
		RuntimeStatus varchar(20) NOT NULL,
		CustomStatusText nvarchar(max) NULL,
		InputText nvarchar(max) NULL,
		OutputText nvarchar(max) NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
	)


IF OBJECT_ID(N'dbo.NewEvents', 'U') IS NULL
    CREATE TABLE NewEvents (
        -- Metadata columns
        SequenceNumber bigint IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewEvents_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_NewEvents_DequeueCount DEFAULT 0,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_NewEvents_Instances_ID FOREIGN KEY REFERENCES Instances(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        RuntimeStatus varchar(30) NULL,
        TaskID int NULL,
        Reason nvarchar(max) NULL,
        PayloadText nvarchar(max) NULL,
    )

IF OBJECT_ID(N'NewTasks', 'U') IS NULL
    CREATE TABLE NewTasks (
        -- Metadata columns
        SequenceNumber bigint CONSTRAINT PK_NewTasks_SequenceNumber PRIMARY KEY IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_NewTasks_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_NewTasks_DequeueCount DEFAULT 0,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_NewTasks_Instances_ID FOREIGN KEY REFERENCES Instances(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        TaskID int NOT NULL,
        PayloadText nvarchar(max) NULL,
    )

IF OBJECT_ID(N'dbo.History', 'U') IS NULL
    CREATE TABLE History (
	    InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_History_Instances_ID FOREIGN KEY REFERENCES Instances(ID) ON DELETE CASCADE,
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

IF OBJECT_ID('CreateInstances') IS NULL
    EXEC('CREATE PROCEDURE CreateInstances AS SET NOCOUNT ON')
GO

ALTER PROCEDURE CreateInstances
    @NewInstanceEvents TaskEvents READONLY
AS
BEGIN
    BEGIN TRANSACTION

    DECLARE @existingStatus varchar(30) = (
        SELECT TOP 1 existing.RuntimeStatus
        FROM Instances existing WITH (HOLDLOCK)
        INNER JOIN @NewInstanceEvents new ON new.InstanceID = existing.ID
    )

    -- CONSIDER: Allow overwriting if the instance is not active (i.e. dedupe status preferences)
    IF @existingStatus IS NOT NULL
        THROW 50001, N'An instance with this ID already exists.', 1;

    INSERT INTO Instances (
        ID,
        ExecutionID,
        [Name],
        RuntimeStatus,
        InputText
    )
    SELECT
        InstanceID,
        ExecutionID,
        [Name],
        ISNULL(RuntimeStatus, 'Pending'),
        PayloadText
    FROM @NewInstanceEvents

    INSERT INTO NewEvents (
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        TaskID,
        [Name],
        PayloadText
    )
    SELECT
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        TaskID,
        [Name],
        PayloadText
    FROM @NewInstanceEvents

    COMMIT TRANSACTION
END
GO


IF OBJECT_ID('QuerySingleOrchestration') IS NULL
    EXEC('CREATE PROCEDURE QuerySingleOrchestration AS SET NOCOUNT ON')
GO

ALTER PROCEDURE QuerySingleOrchestration
    @InstanceID nvarchar(100),
    @ExecutionID nvarchar(100),
    @FetchInput bit = 1,
    @FetchOutput bit = 1
AS
BEGIN
    SELECT TOP 1
        ID AS InstanceID,
        ExecutionID,
        [Name],
        CreatedTime,
        LastUpdatedTime,
        CompletedTime,
        RuntimeStatus,
        CustomStatusText,
        CASE WHEN @FetchInput = 1 THEN InputText ELSE NULL END AS InputText,
        CASE WHEN @FetchOutput = 1 THEN OutputText ELSE NULL END AS OutputText
    FROM Instances
    WHERE
        ID = @InstanceID AND
        (@ExecutionID IS NULL OR @ExecutionID = ExecutionID)
END
GO

IF OBJECT_ID('LockNextOrchestration') IS NULL
    EXEC('CREATE PROCEDURE LockNextOrchestration AS SET NOCOUNT ON')
GO

ALTER PROCEDURE LockNextOrchestration
    @BatchSize int,
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    BEGIN TRANSACTION

    DECLARE @now datetime2 = SYSUTCDATETIME()
    DECLARE @instanceID nvarchar(100)

    -- Lock the first active instance that has pending messages.
    -- Delayed events from durable timers will have a non-null VisibleTime value.
    -- Non-active instances will never have their messages or history read.
    UPDATE TOP (1) Instances
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        @instanceID = ID
    FROM 
        Instances I INNER JOIN NewEvents E ON E.InstanceID = I.ID
    WHERE
        I.RuntimeStatus IN ('Pending', 'Running') AND
	    (I.LockExpiration IS NULL OR I.LockExpiration < @now) AND
        (E.VisibleTime IS NULL OR E.VisibleTime < @now)

    -- Result #1: The list of new events to fetch.
    -- IMPORTANT: DO NOT CHANGE THE ORDER OF RETURNED COLUMNS!
    -- TODO: Update the dequeue count
    SELECT TOP (@BatchSize)
        -- Metadata columns
        SequenceNumber,
        Timestamp,
        VisibleTime,
        DequeueCount,
        -- Orchestration columns
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText
    FROM NewEvents
    WHERE InstanceID = @instanceID

    -- Result #2: The full event history for the locked instance
    SELECT *
    FROM History
    WHERE InstanceID = @instanceID

    COMMIT TRANSACTION
END
GO


IF OBJECT_ID('CheckpointOrchestration') IS NULL
    EXEC('CREATE PROCEDURE CheckpointOrchestration AS SET NOCOUNT ON');
GO

ALTER PROCEDURE CheckpointOrchestration
    @NewOrchestrationEvents TaskEvents READONLY,
    @NewHistoryEvents TaskEvents READONLY,
    @NewTaskEvents TaskEvents READONLY,
    @UpdatedInstanceStatus TaskEvents READONLY,
    @DeletedControlMessages TaskEvents READONLY
AS
BEGIN
    SET NOCOUNT ON
    BEGIN TRANSACTION
    
    UPDATE Instances
    SET
        ExecutionID = new.ExecutionID,
        CustomStatusText = new.CustomStatusText,
        RuntimeStatus = new.RuntimeStatus,
        OutputText = new.PayloadText,
        LastUpdatedTime = SYSUTCDATETIME(),
        CompletedTime = new.CompletedTime,
        LockExpiration = new.LockExpiration
    FROM
        Instances existing
        INNER JOIN @UpdatedInstanceStatus new ON new.InstanceID = existing.ID

    INSERT INTO NewEvents (
        -- Metadata columns
        VisibleTime,
        -- Orchestration columns
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText
    ) 
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText
    FROM @NewOrchestrationEvents

    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE new
    OUTPUT DELETED.InstanceID, DELETED.SequenceNumber
    FROM NewEvents new
        INNER JOIN @DeletedControlMessages M ON M.InstanceID = new.InstanceID AND M.SequenceNumber = new.SequenceNumber

    INSERT INTO History (
        InstanceID,
        SequenceNumber,
        ExecutionID,
        EventType,
        TaskID,
        IsPlayed,
        [Name],
        PayloadText,
        RuntimeStatus,
        VisibleTime)
    SELECT
        InstanceID,
        SequenceNumber,
        ExecutionID,
        EventType,
        TaskID,
        IsPlayed,
        [Name],
        PayloadText,
        RuntimeStatus,
        VisibleTime
    FROM @NewHistoryEvents

    -- TaskScheduled events
    INSERT INTO NewTasks (
        -- Metadata columns
        VisibleTime,
        -- Orchestration columns
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText
    )
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText
    FROM @NewTaskEvents

    COMMIT TRANSACTION
END
GO


IF OBJECT_ID('LockNextTask') IS NULL
    EXEC('CREATE PROCEDURE LockNextTask AS SET NOCOUNT ON')
GO

ALTER PROCEDURE LockNextTask
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()

    -- Update (lock) and return a single row.
    -- The PK_NewTasks_SequenceNumber hint is specified to help ensure in-order selection.
    UPDATE TOP (1) NewTasks WITH (READPAST)
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        DequeueCount = DequeueCount + 1
    OUTPUT
        INSERTED.*
    FROM
        NewTasks WITH (INDEX (PK_NewTasks_SequenceNumber))
    WHERE
	    (LockExpiration IS NULL OR LockExpiration < @now) AND
        (VisibleTime IS NULL OR VisibleTime < @now)
END
GO


IF OBJECT_ID('CompleteTasks') IS NULL
    EXEC('CREATE PROCEDURE CompleteTasks AS SET NOCOUNT ON');
GO

ALTER PROCEDURE CompleteTasks
    @CompletedTasks TaskEvents READONLY,
    @Results TaskEvents READONLY
AS
BEGIN
    BEGIN TRANSACTION
    
    INSERT INTO NewEvents (
        -- Metadata columns
        VisibleTime,
        -- Orchestration columns
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText
    ) 
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText
    FROM @Results

    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE NT
    OUTPUT DELETED.SequenceNumber
    FROM NewTasks NT INNER JOIN @CompletedTasks CT ON CT.SequenceNumber = NT.SequenceNumber

    COMMIT TRANSACTION
END