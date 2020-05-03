-- STORED PROCEDURES (a.k.a. "sprocs")
-- 
-- This file always contains the latest version of the procedure code.
--
-- This file must be idempotent i.e. can be run multiple times with the 
-- exact same outcome. It also must be executable without any downtime.
-- This can be achieved using the CREATE OR ALTER PROCEDURE syntax.

CREATE OR ALTER PROCEDURE dt.CreateInstances
    @NewInstanceEvents TaskEvents READONLY
AS
BEGIN
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
END
GO


CREATE OR ALTER PROCEDURE dt.QuerySingleOrchestration
    @InstanceID nvarchar(100),
    @ExecutionID nvarchar(100) = NULL,
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


CREATE OR ALTER PROCEDURE dt.LockNextOrchestration
    @BatchSize int,
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()
    DECLARE @instanceID nvarchar(100)

    -- Lock the first active instance that has pending messages.
    -- Delayed events from durable timers will have a non-null VisibleTime value.
    -- Non-active instances will never have their messages or history read.
    UPDATE TOP (1) Instances WITH (READPAST)
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        @instanceID = ID
    FROM 
        Instances I INNER JOIN NewEvents E WITH (READPAST) ON E.InstanceID = I.ID
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
        PayloadText,
        DATEDIFF(millisecond, [Timestamp], @now) AS WaitTime
    FROM NewEvents WITH (READPAST)
    WHERE InstanceID = @instanceID

    -- Result #2: The full event history for the locked instance
    SELECT *
    FROM History
    WHERE InstanceID = @instanceID
END
GO


CREATE OR ALTER PROCEDURE dt.CheckpointOrchestration
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
        PayloadText,
        LockedBy,
        LockExpiration
    )
    OUTPUT
        inserted.SequenceNumber,
        inserted.TaskID
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText,
        LockedBy,
        LockExpiration
    FROM @NewTaskEvents

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt.LockNextTask
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()

    -- Update (lock) and return a single row.
    -- The PK_NewTasks_SequenceNumber hint is specified to help ensure in-order selection.
    -- TODO: Filter out tasks for instances that are in a non-running state (terminated, suspended, etc.)
    UPDATE TOP (1) NewTasks WITH (READPAST)
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        DequeueCount = DequeueCount + 1
    OUTPUT
        INSERTED.*,
        DATEDIFF(millisecond, INSERTED.[Timestamp], @now) AS WaitTime
    FROM
        NewTasks WITH (INDEX (PK_NewTasks_SequenceNumber))
    WHERE
	    (LockExpiration IS NULL OR LockExpiration < @now) AND
        (VisibleTime IS NULL OR VisibleTime < @now)
END
GO


CREATE OR ALTER PROCEDURE dt.CompleteTasks
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
GO


CREATE OR ALTER PROCEDURE dt.GetVersions
AS
BEGIN
    SELECT SemanticVersion, UpgradeTime
    FROM Versions
    ORDER BY UpgradeTime DESC
END
GO


CREATE OR ALTER PROCEDURE dt.UpdateVersion
    @SemanticVersion nvarchar(100)
AS
BEGIN
    -- Duplicates are ignored (per the schema definition of dt.Versions)
    INSERT INTO Versions (SemanticVersion)
    VALUES (@SemanticVersion)
END
GO
