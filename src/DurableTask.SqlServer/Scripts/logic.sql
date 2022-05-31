-- Copyright (c) .NET Foundation. All rights reserved.
-- Licensed under the MIT License. See LICENSE in the project root for license information.

CREATE OR ALTER FUNCTION dt.CurrentTaskHub()
    RETURNS varchar(50)
    WITH EXECUTE AS CALLER
AS
BEGIN
    -- Task Hub modes:
    -- 0: Task hub names are set by the app
    -- 1: Task hub names are inferred from the user credential
    DECLARE @taskHubMode sql_variant = (SELECT TOP 1 [Value] FROM dt.GlobalSettings WHERE [Name] = 'TaskHubMode');

    DECLARE @taskHub varchar(150)

    IF @taskHubMode = 0
        SET @taskHub = APP_NAME()
    IF @taskHubMode = 1
        SET @taskHub = USER_NAME()

    IF @taskHub IS NULL
        SET @taskHub = 'default'

    -- if the name is too long, keep the first 16 characters and hash the rest
    IF LEN(@taskHub) > 50
        SET @taskHub = CONVERT(varchar(16), @taskHub) + '__' + CONVERT(varchar(32), HASHBYTES('MD5', @taskHub), 2)

    RETURN @taskHub
END
GO


CREATE OR ALTER FUNCTION dt.GetScaleMetric()
    RETURNS INT
    WITH EXECUTE AS CALLER
AS
BEGIN
    DECLARE @taskHub varchar(50) = dt.CurrentTaskHub()
    DECLARE @now datetime2 = SYSUTCDATETIME()

    DECLARE @liveInstances int = 0
    DECLARE @liveTasks int = 0

    SELECT
        @liveInstances = COUNT(DISTINCT E.[InstanceID]),
        @liveTasks = COUNT(T.[InstanceID])
    FROM dt.Instances I WITH (NOLOCK)
        LEFT OUTER JOIN dt.NewEvents E WITH (NOLOCK) ON E.[TaskHub] = @taskHub AND E.[InstanceID] = I.[InstanceID]
        LEFT OUTER JOIN dt.NewTasks T WITH (NOLOCK) ON T.[TaskHub] = @taskHub AND T.[InstanceID] = I.[InstanceID]
    WHERE
        I.[TaskHub] = @taskHub
        AND I.[RuntimeStatus] IN ('Pending', 'Running')
        AND (E.[VisibleTime] IS NULL OR @now > E.[VisibleTime])

    RETURN @liveInstances + @liveTasks
END
GO


CREATE OR ALTER FUNCTION dt.GetScaleRecommendation(@MaxOrchestrationsPerWorker real, @MaxActivitiesPerWorker real)
    RETURNS INT
    WITH EXECUTE AS CALLER
AS
BEGIN
    DECLARE @taskHub varchar(50) = dt.CurrentTaskHub()
    DECLARE @now datetime2 = SYSUTCDATETIME()

    DECLARE @liveInstances int = 0
    DECLARE @liveTasks int = 0

    SELECT
        @liveInstances = COUNT(DISTINCT E.[InstanceID]),
        @liveTasks = COUNT(T.[InstanceID])
    FROM dt.Instances I WITH (NOLOCK)
        LEFT OUTER JOIN dt.NewEvents E WITH (NOLOCK) ON E.[TaskHub] = @taskHub AND E.[InstanceID] = I.[InstanceID]
        LEFT OUTER JOIN dt.NewTasks T WITH (NOLOCK) ON T.[TaskHub] = @taskHub AND T.[InstanceID] = I.[InstanceID]
    WHERE
        I.[TaskHub] = @taskHub
        AND I.[RuntimeStatus] IN ('Pending', 'Running')
        AND (E.[VisibleTime] IS NULL OR E.[VisibleTime] < @now)

    IF @MaxOrchestrationsPerWorker < 1 SET @MaxOrchestrationsPerWorker = 1
    IF @MaxActivitiesPerWorker < 1 SET @MaxActivitiesPerWorker = 1

    DECLARE @recommendedWorkersForOrchestrations int = CEILING(@liveInstances / @MaxOrchestrationsPerWorker)
    DECLARE @recommendedWorkersForActivities int = CEILING(@liveTasks / @MaxActivitiesPerWorker)

    RETURN @recommendedWorkersForOrchestrations + @recommendedWorkersForActivities
END
GO


CREATE OR ALTER VIEW dt.vInstances
AS
    SELECT
        I.[TaskHub],
        I.[InstanceID],
        I.[ExecutionID],
        I.[Name],
        I.[Version],
        I.[CreatedTime],
        I.[LastUpdatedTime],
        I.[CompletedTime],
        I.[RuntimeStatus],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = dt.CurrentTaskHub() AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[CustomStatusPayloadID]) AS [CustomStatusText],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = dt.CurrentTaskHub() AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[InputPayloadID]) AS [InputText],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE 
            P.[TaskHub] = dt.CurrentTaskHub() AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[OutputPayloadID]) AS [OutputText]
    FROM Instances I
    WHERE
        I.[TaskHub] = dt.CurrentTaskHub()
GO

CREATE OR ALTER VIEW dt.vHistory
AS
    SELECT
        H.[TaskHub],
        H.[InstanceID],
	    H.[ExecutionID],
        H.[SequenceNumber],
	    H.[EventType],
	    H.[TaskID],
	    H.[Timestamp],
	    H.[IsPlayed],
	    H.[Name],
	    H.[RuntimeStatus],
        H.[VisibleTime],
	    (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = dt.CurrentTaskHub() AND
            P.[InstanceID] = H.[InstanceID] AND
            P.[PayloadID] = H.[DataPayloadID]) AS [Payload]
    FROM History H
    WHERE
        H.[TaskHub] = dt.CurrentTaskHub()
GO


CREATE OR ALTER PROCEDURE dt.CreateInstance
    @Name varchar(300),
    @Version varchar(100) = NULL,
    @InstanceID varchar(100) = NULL,
    @ExecutionID varchar(50) = NULL,
    @InputText varchar(MAX) = NULL,
    @StartTime datetime2 = NULL
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()
    DECLARE @EventType varchar(30) = 'ExecutionStarted'
    DECLARE @RuntimeStatus varchar(30) = 'Pending'

    -- Check for instance ID collisions
    IF @InstanceID IS NULL
    BEGIN
        SET @InstanceID = NEWID()
    END
    ELSE
    BEGIN
        BEGIN TRANSACTION

        DECLARE @existingStatus varchar(30) = (
            SELECT TOP 1 existing.[RuntimeStatus]
            FROM Instances existing WITH (HOLDLOCK)
            WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
        )

        -- Instance IDs can be overwritten only if the orchestration is in a terminal state
        IF @existingStatus IN ('Pending', 'Running')
        BEGIN
            DECLARE @msg nvarchar(4000) = FORMATMESSAGE('Cannot create instance with ID ''%s'' because a pending or running instance with ID already exists.', @InstanceId);
            THROW 50001, @msg, 1;
        END
        ELSE IF @existingStatus IS NOT NULL
        BEGIN
            -- Purge the existing instance data so that it can be overwritten
            DECLARE @instancesToPurge InstanceIDs
            INSERT INTO @instancesToPurge VALUES (@InstanceID)
            EXEC dt.PurgeInstanceStateByID @instancesToPurge
        END

        COMMIT TRANSACTION
    END

    IF @ExecutionID IS NULL
    BEGIN
        SET @ExecutionID = NEWID()
    END

    BEGIN TRANSACTION
    
    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Payloads --> Instances --> NewEvents
    
    DECLARE @InputPayloadID uniqueidentifier
    IF @InputText IS NOT NULL
    BEGIN
        SET @InputPayloadID = NEWID()
        INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text])
        VALUES (@TaskHub, @InstanceID, @InputPayloadID, @InputText)
    END

    INSERT INTO Instances (
        [Name],
        [Version],
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [RuntimeStatus],
        [InputPayloadID])
    VALUES (
        @Name,
        @Version,
        @TaskHub,
        @InstanceID,
        @ExecutionID,
        @RuntimeStatus,
        @InputPayloadID
    )

    INSERT INTO NewEvents (
        [Name],
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [RuntimeStatus],
        [VisibleTime],
        [EventType],
        [PayloadID]
    ) VALUES (
        @Name,
        @TaskHub,
        @InstanceID,
        @ExecutionID,
        @RuntimeStatus,
        @StartTime,
        @EventType,
        @InputPayloadID)

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt.GetInstanceHistory
    @InstanceID varchar(100),
    @GetInputsAndOutputs bit = 0
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()
    DECLARE @ParentInstanceID varchar(100)
    DECLARE @Version varchar(100)
    
    SELECT
        @ParentInstanceID = [ParentInstanceID],
        @Version = [Version]
    FROM Instances WHERE [InstanceID] = @InstanceID

    SELECT
        H.[InstanceID],
        H.[ExecutionID],
        H.[SequenceNumber],
        H.[EventType],
        H.[Name],
        H.[RuntimeStatus],
        H.[TaskID],
        H.[Timestamp],
        H.[IsPlayed],
        H.[VisibleTime],
        P.[Reason],
        (CASE WHEN @GetInputsAndOutputs = 0 THEN NULL ELSE P.[Text] END) AS [PayloadText],
        [PayloadID],
        @ParentInstanceID as [ParentInstanceID],
        @Version as [Version]
    FROM History H WITH (INDEX (PK_History))
        LEFT OUTER JOIN Payloads P ON
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = H.[InstanceID] AND
            P.[PayloadID] = H.[DataPayloadID]
    WHERE
        H.[TaskHub] = @TaskHub AND
        H.[InstanceID] = @InstanceID
    ORDER BY H.[SequenceNumber] ASC
END
GO


CREATE OR ALTER PROCEDURE dt.RaiseEvent
    @Name varchar(300),
    @InstanceID varchar(100) = NULL,
    @PayloadText varchar(MAX) = NULL,
    @DeliveryTime datetime2 = NULL
AS
BEGIN
    BEGIN TRANSACTION

    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    -- External event messages must target new instances or they must use
    -- the "auto start" instance ID format of @orchestrationname@identifier.
    IF NOT EXISTS (
        SELECT 1
        FROM Instances I
        WHERE [TaskHub] = @TaskHub AND I.[InstanceID] = @InstanceID)
    BEGIN
        INSERT INTO Instances (
            [TaskHub],
            [InstanceID],
            [ExecutionID],
            [Name],
            [Version],
            [RuntimeStatus])
        SELECT
            @TaskHub,
            @InstanceID,
            NEWID(),
            SUBSTRING(@InstanceID, 2, CHARINDEX('@', @InstanceID, 2) - 2),
            '',
            'Pending'
        WHERE LEFT(@InstanceID, 1) = '@' AND CHARINDEX('@', @InstanceID, 2) > 2

        -- The instance ID is not auto-start and doesn't already exist, so we fail.
        IF @@ROWCOUNT = 0
            THROW 50000, 'The instance does not exist.', 1; 
    END

    -- Payloads are stored separately from the events
    DECLARE @PayloadID uniqueidentifier = NULL
    IF @PayloadText IS NOT NULL
    BEGIN
        SET @PayloadID = NEWID()
        INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text])
        VALUES (@TaskHub, @InstanceID, @PayloadID, @PayloadText)
    END

    INSERT INTO NewEvents (
        [Name],
        [TaskHub],
        [InstanceID],
        [EventType],
        [VisibleTime],
        [PayloadID]
    ) VALUES (
        @Name,
        @TaskHub,
        @InstanceID,
        'EventRaised',
        @DeliveryTime,
        @PayloadID)

    COMMIT TRANSACTION

END
GO

CREATE OR ALTER PROCEDURE dt.TerminateInstance
    @InstanceID varchar(100),
    @Reason varchar(max) = NULL
AS
BEGIN
    BEGIN TRANSACTION

    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Instances --> (NewEvents --> Payloads --> NewEvents)  

    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    DECLARE @existingStatus varchar(30) = (
        SELECT TOP 1 existing.[RuntimeStatus]
        FROM Instances existing WITH (HOLDLOCK)
        WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
    )

    IF @existingStatus IS NULL
        THROW 50000, 'The instance does not exist.', 1;

    -- If the instance is already completed, no need to terminate it.
    IF @existingStatus IN ('Pending', 'Running')
    BEGIN
        IF NOT EXISTS (
            SELECT TOP (1) 1 FROM NewEvents
            WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID AND [EventType] = 'ExecutionTerminated'
        )
        BEGIN
            -- Payloads are stored separately from the events
            DECLARE @PayloadID uniqueidentifier = NULL
            IF @Reason IS NOT NULL
            BEGIN
                -- Note that we don't use the Reason column for the Reason with terminate events
                SET @PayloadID = NEWID()
                INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text])
                VALUES (@TaskHub, @InstanceID, @PayloadID, @Reason)
            END

            INSERT INTO NewEvents (
                [TaskHub],
                [InstanceID],
                [EventType],
                [PayloadID]
            ) VALUES (
                @TaskHub,
                @InstanceID,
                'ExecutionTerminated',
                @PayloadID)
        END
    END

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt.PurgeInstanceStateByID
    @InstanceIDs InstanceIDs READONLY
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    BEGIN TRANSACTION

    DELETE FROM NewEvents WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DELETE FROM NewTasks  WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DELETE FROM Instances WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DECLARE @deletedInstances int = @@ROWCOUNT
    DELETE FROM History  WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DELETE FROM Payloads WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)

    COMMIT TRANSACTION

    -- return the number of deleted instances
    RETURN @deletedInstances
END
GO


CREATE OR ALTER PROCEDURE dt.PurgeInstanceStateByTime
    @ThresholdTime datetime2,
    @FilterType tinyint = 0
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    DECLARE @instanceIDs InstanceIDs

    IF @FilterType = 0 -- created time
    BEGIN
        INSERT INTO @instanceIDs
            SELECT [InstanceID] FROM Instances
            WHERE [TaskHub] = @TaskHub AND [RuntimeStatus] IN ('Completed', 'Terminated', 'Failed')
                AND [CreatedTime] <= @ThresholdTime
    END
    ELSE IF @FilterType = 1 -- completed time
    BEGIN
        INSERT INTO @instanceIDs
            SELECT [InstanceID] FROM Instances
            WHERE [TaskHub] = @TaskHub AND [RuntimeStatus] IN ('Completed', 'Terminated', 'Failed')
                AND [CompletedTime] <= @ThresholdTime
    END
    ELSE
    BEGIN
        DECLARE @msg nvarchar(100) = FORMATMESSAGE('Unknown or unsupported filter type: %d', @FilterType);
        THROW 50000, @msg, 1;
    END

    DECLARE @deletedInstances int
    EXECUTE @deletedInstances = dt.PurgeInstanceStateByID @instanceIDs
    RETURN @deletedInstances
END
GO

CREATE OR ALTER PROCEDURE dt.SetGlobalSetting
    @Name varchar(300),
    @Value sql_variant
AS
BEGIN
    BEGIN TRANSACTION
 
    UPDATE dt.GlobalSettings WITH (UPDLOCK, HOLDLOCK)
    SET
        [Value] = @Value,
        [Timestamp] = SYSUTCDATETIME(),
        [LastModifiedBy] = USER_NAME()
    WHERE
        [Name] = @Name
 
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO dt.GlobalSettings ([Name], [Value]) VALUES (@Name, @Value)
    END
 
    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._LockNextOrchestration
    @BatchSize int,
    @LockedBy varchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()
    DECLARE @instanceID varchar(100)
    DECLARE @parentInstanceID varchar(100)
    DECLARE @version varchar(100)
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    BEGIN TRANSACTION

    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Instances --> NewEvents --> Payloads --> History

    -- Lock the first active instance that has pending messages.
    -- Delayed events from durable timers will have a non-null VisibleTime value.
    -- Non-active instances will never have their messages or history read.
    UPDATE TOP (1) Instances WITH (READPAST)
    SET
        [LockedBy] = @LockedBy,
	    [LockExpiration] = @LockExpiration,
        @instanceID = I.[InstanceID],
        @parentInstanceID = I.[ParentInstanceID],
        @version = I.[Version]
    FROM 
        dt.Instances I WITH (READPAST) INNER JOIN NewEvents E WITH (READPAST) ON
            E.[TaskHub] = @TaskHub AND
            E.[InstanceID] = I.[InstanceID]
    WHERE
        I.TaskHub = @TaskHub AND
        I.[RuntimeStatus] IN ('Pending', 'Running') AND
	    (I.[LockExpiration] IS NULL OR I.[LockExpiration] < @now) AND
        (E.[VisibleTime] IS NULL OR E.[VisibleTime] < @now)

    -- Result #1: The list of new events to fetch.
    -- IMPORTANT: DO NOT CHANGE THE ORDER OF RETURNED COLUMNS!
    -- TODO: Update the dequeue count
    SELECT TOP (@BatchSize)
        N.[SequenceNumber],
        N.[Timestamp],
        N.[VisibleTime],
        N.[DequeueCount],
        N.[InstanceID],
        N.[ExecutionID],
        N.[EventType],
        N.[Name],
        N.[RuntimeStatus],
        N.[TaskID],
        P.[Reason],
        P.[Text] AS [PayloadText],
        P.[PayloadID],
        DATEDIFF(SECOND, [Timestamp], @now) AS [WaitTime],
        @parentInstanceID as [ParentInstanceID],
        @version as [Version]
    FROM NewEvents N
        LEFT OUTER JOIN dt.[Payloads] P ON 
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = N.[InstanceID] AND
            P.[PayloadID] = N.[PayloadID]
    WHERE
        N.[TaskHub] = @TaskHub AND
        N.[InstanceID] = @instanceID AND
        (N.[VisibleTime] IS NULL OR N.[VisibleTime] < @now)

    -- Bail if no events are returned - this implies that another thread already took them (???)
    IF @@ROWCOUNT = 0
    BEGIN
        ROLLBACK TRANSACTION
        RETURN
    END

    -- Result #2: The full event history for the locked instance
    -- NOTE: This must be kept consistent with the dt.HistoryEvents custom data type
    SELECT
        H.[InstanceID],
        H.[ExecutionID],
        H.[SequenceNumber],
        H.[EventType],
        H.[Name],
        H.[RuntimeStatus],
        H.[TaskID],
        H.[Timestamp],
        H.[IsPlayed],
        H.[VisibleTime],
        P.[Reason],
        -- Optimization: Do not load the data payloads for these history events - they are not needed since they are never replayed
        (CASE WHEN [EventType] IN ('TaskScheduled', 'SubOrchestrationInstanceCreated') THEN NULL ELSE P.[Text] END) AS [PayloadText],
        [PayloadID],
        @parentInstanceID as [ParentInstanceID],
        @version as [Version]
    FROM History H WITH (INDEX (PK_History))
        LEFT OUTER JOIN Payloads P ON
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = H.[InstanceID] AND
            P.[PayloadID] = H.[DataPayloadID]
    WHERE H.[TaskHub] = @TaskHub AND H.[InstanceID] = @instanceID
    ORDER BY H.[SequenceNumber] ASC

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._CheckpointOrchestration
    @InstanceID varchar(100),
    @ExecutionID varchar(50),
    @RuntimeStatus varchar(30),
    @CustomStatusPayload varchar(MAX),
    @DeletedEvents MessageIDs READONLY,
    @NewHistoryEvents HistoryEvents READONLY,
    @NewOrchestrationEvents OrchestrationEvents READONLY,
    @NewTaskEvents TaskEvents READONLY
AS
BEGIN
    BEGIN TRANSACTION

    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    DECLARE @InputPayloadID uniqueidentifier
    DECLARE @CustomStatusPayloadID uniqueidentifier
    DECLARE @ExistingCustomStatusPayload varchar(MAX)
    DECLARE @ExistingExecutionID varchar(50)

    -- Check for an existing custom status payload for this instance. If one exists, compare it
    -- to the new one to know if we need to update the existing entry or not.
    -- At the same time, grab the execution ID so we can learn if this is a ContinueAsNew.
    SELECT TOP 1 
        @InputPayloadID = I.[InputPayloadID],
        @CustomStatusPayloadID = I.[CustomStatusPayloadID],
        @ExistingCustomStatusPayload = P.[Text],
        @ExistingExecutionID = I.[ExecutionID]
    FROM Payloads P RIGHT OUTER JOIN Instances I ON
        P.[TaskHub] = @TaskHub AND
        P.[InstanceID] = I.[InstanceID] AND
        P.[PayloadID] = I.[CustomStatusPayloadID]
    WHERE I.[TaskHub] = @TaskHub AND I.[InstanceID] = @InstanceID

    -- ContinueAsNew case: delete all existing state
    DECLARE @IsContinueAsNew BIT = 0
    IF @ExistingExecutionID IS NOT NULL AND @ExistingExecutionID <> @ExecutionID
    BEGIN
        DECLARE @DeletedPayloadIDs TABLE (PayloadID uniqueidentifier, InstanceID varchar(100))

        DELETE FROM History
        OUTPUT deleted.[DataPayloadID], deleted.[InstanceID] INTO @DeletedPayloadIDs
        WHERE [TaskHub] = @TaskHub AND InstanceID = @InstanceID

        DELETE FROM Payloads
        FROM Payloads P WITH (FORCESEEK(PK_Payloads(TaskHub, InstanceID))) INNER JOIN @DeletedPayloadIDs D ON
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = D.[InstanceID] AND
            P.[PayloadID] = D.[PayloadID]
        WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = @InstanceID AND
            P.[PayloadID] NOT IN (@CustomStatusPayloadID, @InputPayloadID)

        SET @IsContinueAsNew = 1
    END

    -- Custom status case #1: Setting the custom status for the first time
    IF @ExistingCustomStatusPayload IS NULL AND @CustomStatusPayload IS NOT NULL
    BEGIN
        SET @CustomStatusPayloadID = NEWID()
        INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text])
        VALUES (@TaskHub, @InstanceID, @CustomStatusPayloadID, @CustomStatusPayload)
    END

    -- Custom status case #2: Updating an existing custom status payload
    IF @ExistingCustomStatusPayload IS NOT NULL AND @ExistingCustomStatusPayload <> @CustomStatusPayload
    BEGIN
        UPDATE Payloads SET [Text] = @CustomStatusPayload WHERE 
            [TaskHub] = @TaskHub AND
            [InstanceID] = @InstanceID AND
            [PayloadID] = @CustomStatusPayloadID
    END

    -- Need to update the input payload ID if this is a ContinueAsNew
    IF @IsContinueAsNew = 1
    BEGIN
        SET @InputPayloadID = (
            SELECT TOP 1 [PayloadID]
            FROM @NewHistoryEvents
            WHERE [EventType] = 'ExecutionStarted'
            ORDER BY [SequenceNumber] DESC
        )
    END

    DECLARE @IsCompleted bit
    SET @IsCompleted = (CASE WHEN @RuntimeStatus IN ('Completed', 'Failed', 'Terminated') THEN 1 ELSE 0 END)

    -- The output payload will only exist when the orchestration has completed.
    -- Fetch it's payload ID now so that we can update it in the Instances table further down.
    DECLARE @OutputPayloadID uniqueidentifier
    IF @IsCompleted = 1
    BEGIN
        SET @OutputPayloadID = (
            SELECT TOP 1 [PayloadID]
            FROM @NewHistoryEvents
            WHERE [EventType] = 'ExecutionCompleted' OR [EventType] = 'ExecutionTerminated'
            ORDER BY [SequenceNumber] DESC
        )
    END

    -- Insert data payloads into the Payloads table as a single statement.
    -- The [PayloadText] value will be NULL if there is no payload or if a payload is already known to exist in the DB.
    -- The [PayloadID] value might be set even if [PayloadText] and [Reason] are both NULL.
    -- This needs to be done before the UPDATE to Instances because the Instances table needs to reference the output payload.
    INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text], [Reason])
        SELECT @TaskHub, [InstanceID], [PayloadID], [PayloadText], [Reason]
        FROM @NewHistoryEvents
        WHERE [PayloadText] IS NOT NULL OR [Reason] IS NOT NULL

    UPDATE Instances
    SET
        [ExecutionID] = @ExecutionID,
        [RuntimeStatus] = @RuntimeStatus,
        [LastUpdatedTime] = SYSUTCDATETIME(),
        [CompletedTime] = (CASE WHEN @IsCompleted = 1 THEN SYSUTCDATETIME() ELSE NULL END),
        [LockExpiration] = NULL, -- release the lock
        [CustomStatusPayloadID] = @CustomStatusPayloadID,
        [InputPayloadID] = @InputPayloadID,
        [OutputPayloadID] = @OutputPayloadID
    FROM Instances
    WHERE [TaskHub] = @TaskHub and [InstanceID] = @InstanceID

    IF @@ROWCOUNT = 0
        THROW 50000, 'The instance does not exist.', 1;

    -- External event messages can create new instances
    -- NOTE: There is a chance this could result in deadlocks if two 
    --       instances are sending events to each other at the same time
    INSERT INTO Instances (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [Name],
        [Version],
        [RuntimeStatus])
    SELECT DISTINCT
        @TaskHub,
        E.[InstanceID],
        NEWID(),
        SUBSTRING(E.[InstanceID], 2, CHARINDEX('@', E.[InstanceID], 2) - 2),
        '',
        'Pending'
    FROM @NewOrchestrationEvents E
    WHERE LEFT(E.[InstanceID], 1) = '@'
        AND CHARINDEX('@', E.[InstanceID], 2) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM dt.Instances I
            WHERE [TaskHub] = @TaskHub AND I.[InstanceID] = E.[InstanceID])
    GROUP BY E.[InstanceID]
    ORDER BY E.[InstanceID] ASC

    -- Create sub-orchestration instances
    INSERT INTO Instances (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [Name],
        [Version],
        [ParentInstanceID],
        [RuntimeStatus])
    SELECT DISTINCT
        @TaskHub,
        E.[InstanceID],
        E.[ExecutionID],
        E.[Name],
        E.[Version],
        E.[ParentInstanceID],
        'Pending'
    FROM @NewOrchestrationEvents E
    WHERE E.[EventType] IN ('ExecutionStarted')
        AND NOT EXISTS (
            SELECT 1
            FROM dt.Instances I
            WHERE [TaskHub] = @TaskHub AND I.[InstanceID] = E.[InstanceID])
    ORDER BY E.[InstanceID] ASC

    -- Insert new event data payloads into the Payloads table in batches.
    -- PayloadID values are provided by the caller only if a payload exists.
    INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text], [Reason])
        SELECT @TaskHub, [InstanceID], [PayloadID], [PayloadText], [Reason]
        FROM @NewOrchestrationEvents
        WHERE [PayloadID] IS NOT NULL

    INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text])
        SELECT @TaskHub, [InstanceID], [PayloadID], [PayloadText]
        FROM @NewTaskEvents
        WHERE [PayloadID] IS NOT NULL

    -- Insert the new events with references to their payloads, if applicable
    INSERT INTO NewEvents (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [EventType],
        [Name],
        [RuntimeStatus],
        [VisibleTime],
        [TaskID],
        [PayloadID]
    ) 
    SELECT 
        @TaskHub,
        [InstanceID],
        [ExecutionID],
        [EventType],
        [Name],
        [RuntimeStatus],
        [VisibleTime],
        [TaskID],
        [PayloadID]
    FROM @NewOrchestrationEvents
    
    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE E
    OUTPUT DELETED.InstanceID, DELETED.SequenceNumber
    FROM dt.NewEvents E WITH (FORCESEEK(PK_NewEvents(TaskHub, InstanceID, SequenceNumber)))
        INNER JOIN @DeletedEvents D ON 
            D.InstanceID = E.InstanceID AND
            D.SequenceNumber = E.SequenceNumber AND
            E.TaskHub = @TaskHub

    -- IMPORTANT: This insert is expected to fail with a primary key constraint violation in a
    --            split-brain situation where two instances try to execute the same orchestration
    --            at the same time. The SDK will check for this exact error condition.
    INSERT INTO History (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [SequenceNumber],
        [EventType],
        [TaskID],
        [Timestamp],
        [IsPlayed],
        [Name],
        [RuntimeStatus],
        [VisibleTime],
        [DataPayloadID])
    SELECT
        @TaskHub,
        H.[InstanceID],
        H.[ExecutionID],
        H.[SequenceNumber],
        H.[EventType],
        H.[TaskID],
        H.[Timestamp],
        H.[IsPlayed],
        H.[Name],
        H.[RuntimeStatus],
        H.[VisibleTime],
        H.[PayloadID]
    FROM @NewHistoryEvents H

    -- TaskScheduled events
    INSERT INTO NewTasks (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [Name],
        [TaskID],
        [VisibleTime],
        [LockedBy],
        [LockExpiration],
        [PayloadID],
        [Version]
    )
    OUTPUT
        INSERTED.[SequenceNumber],
        INSERTED.[TaskID]
    SELECT 
        @TaskHub,
        [InstanceID],
        [ExecutionID],
        [Name],
        [TaskID],
        [VisibleTime],
        [LockedBy],
        [LockExpiration],
        [PayloadID],
        [Version]
    FROM @NewTaskEvents

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._AddOrchestrationEvents
    @NewOrchestrationEvents OrchestrationEvents READONLY 
AS
BEGIN
    BEGIN TRANSACTION

    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Payloads --> NewEvents

    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()
    
    -- External event messages can create new instances
    -- NOTE: There is a chance this could result in deadlocks if two 
    --       instances are sending events to each other at the same time
    BEGIN TRY
        INSERT INTO Instances (
            [TaskHub],
            [InstanceID],
            [ExecutionID],
            [Name],
            [Version],
            [RuntimeStatus])
        SELECT DISTINCT
            @TaskHub,
            E.[InstanceID],
            NEWID(),
            SUBSTRING(E.[InstanceID], 2, CHARINDEX('@', E.[InstanceID], 2) - 2),
            '',
            'Pending'
        FROM @NewOrchestrationEvents E
        WHERE NOT EXISTS (
            SELECT 1
            FROM Instances I
            WHERE [TaskHub] = @TaskHub AND I.[InstanceID] = E.[InstanceID])
        GROUP BY E.[InstanceID]
        ORDER BY E.[InstanceID] ASC
    END TRY
    BEGIN CATCH
        -- Ignore PK violations here, which can happen when multiple clients
        -- try to add messages at the same time for the same instance
        IF ERROR_NUMBER() <> 2627  -- 2627 is PK violation
            THROW
    END CATCH

    -- Insert new event data payloads into the Payloads table in batches.
    -- PayloadID values are provided by the caller only if a payload exists.
    INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text], [Reason])
        SELECT @TaskHub, [InstanceID], [PayloadID], [PayloadText], [Reason]
        FROM @NewOrchestrationEvents
        WHERE [PayloadID] IS NOT NULL

    -- Insert the new events with references to their payloads, if applicable
    INSERT INTO NewEvents (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [EventType],
        [Name],
        [RuntimeStatus],
        [VisibleTime],
        [TaskID],
        [PayloadID]
    ) 
    SELECT 
        @TaskHub,
        [InstanceID],
        [ExecutionID],
        [EventType],
        [Name],
        [RuntimeStatus],
        [VisibleTime],
        [TaskID],
        [PayloadID]
    FROM @NewOrchestrationEvents

    COMMIT TRANSACTION
END
GO

CREATE OR ALTER PROCEDURE dt.QuerySingleOrchestration
    @InstanceID varchar(100),
    @ExecutionID varchar(50) = NULL,
    @FetchInput bit = 1,
    @FetchOutput bit = 1
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    SELECT TOP 1
        I.[InstanceID],
        I.[ExecutionID],
        I.[Name],
        I.[Version],
        I.[CreatedTime],
        I.[LastUpdatedTime],
        I.[CompletedTime],
        I.[RuntimeStatus],
        I.[ParentInstanceID],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[CustomStatusPayloadID]) AS [CustomStatusText],
        CASE WHEN @FetchInput = 1 THEN (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[InputPayloadID]) ELSE NULL END AS [InputText],
        CASE WHEN @FetchOutput = 1 THEN (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[OutputPayloadID]) ELSE NULL END AS [OutputText]
    FROM Instances I
    WHERE
        I.[TaskHub] = @TaskHub AND
        I.[InstanceID] = @InstanceID AND
        (@ExecutionID IS NULL OR @ExecutionID = I.ExecutionID)
END
GO


CREATE OR ALTER PROCEDURE dt._QueryManyOrchestrations
    @PageSize smallint = 100,
    @PageNumber smallint = 0,
    @FetchInput bit = 1,
    @FetchOutput bit = 1,
    @CreatedTimeFrom datetime2 = NULL,
    @CreatedTimeTo datetime2 = NULL,
    @RuntimeStatusFilter varchar(200) = NULL,
    @InstanceIDPrefix varchar(100) = NULL
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    SELECT
        I.[InstanceID],
        I.[ExecutionID],
        I.[Name],
        I.[Version],
        I.[CreatedTime],
        I.[LastUpdatedTime],
        I.[CompletedTime],
        I.[RuntimeStatus],
        I.[ParentInstanceID],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[CustomStatusPayloadID]) AS [CustomStatusText],
        CASE WHEN @FetchInput = 1 THEN (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[InputPayloadID]) ELSE NULL END AS [InputText],
        CASE WHEN @FetchOutput = 1 THEN (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[OutputPayloadID]) ELSE NULL END AS [OutputText]
    FROM
        Instances I
    WHERE
        I.[TaskHub] = @TaskHub AND
        (@CreatedTimeFrom IS NULL OR I.[CreatedTime] >= @CreatedTimeFrom) AND
        (@CreatedTimeTo IS NULL OR I.[CreatedTime] <= @CreatedTimeTo) AND
        (@RuntimeStatusFilter IS NULL OR I.[RuntimeStatus] IN (SELECT [value] FROM string_split(@RuntimeStatusFilter, ','))) AND
        (@InstanceIDPrefix IS NULL OR I.[InstanceID] LIKE @InstanceIDPrefix + '%')
    ORDER BY
        I.[CreatedTime] OFFSET (@PageNumber * @PageSize) ROWS FETCH NEXT @PageSize ROWS ONLY
END
GO


CREATE OR ALTER PROCEDURE dt._LockNextTask
    @LockedBy varchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()
    DECLARE @now datetime2 = SYSUTCDATETIME()

    DECLARE @SequenceNumber bigint

    BEGIN TRANSACTION
    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: NewTasks --> Payloads

    -- Update (lock) and return a single row.
    -- The PK_NewTasks hint is specified to help ensure in-order selection.
    -- TODO: Filter out tasks for instances that are in a non-running state (suspended, etc.)
    UPDATE TOP (1) NewTasks WITH (READPAST)
    SET
        @SequenceNumber = [SequenceNumber],
        [LockedBy] = @LockedBy,
	    [LockExpiration] = @LockExpiration,
        [DequeueCount] = [DequeueCount] + 1
    FROM
        NewTasks WITH (INDEX (PK_NewTasks))
    WHERE
        [TaskHub] = @TaskHub AND
	    ([LockExpiration] IS NULL OR [LockExpiration] < @now) AND
        ([VisibleTime] IS NULL OR [VisibleTime] < @now)

    SELECT TOP (1)
        [SequenceNumber],
        [InstanceID],
        [ExecutionID],
        [Name],
        'TaskScheduled' AS [EventType],
        [TaskID],
        [VisibleTime],
        [Timestamp],
        [DequeueCount],
        [Version],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = N.[InstanceID] AND
            P.[PayloadID] = N.[PayloadID]) AS [PayloadText],
        DATEDIFF(SECOND, [Timestamp], @now) AS [WaitTime]
    FROM NewTasks N
    WHERE [TaskHub] = @TaskHub AND [SequenceNumber] = @SequenceNumber

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._RenewOrchestrationLocks
    @InstanceID varchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    UPDATE Instances
    SET [LockExpiration] = @LockExpiration
    WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
END
GO


CREATE OR ALTER PROCEDURE dt._RenewTaskLocks
    @RenewingTasks MessageIDs READONLY,
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    UPDATE N
    SET [LockExpiration] = @LockExpiration
    FROM NewTasks N INNER JOIN @RenewingTasks C ON
        C.[SequenceNumber] = N.[SequenceNumber] AND
        N.[TaskHub] = @TaskHub
END
GO


CREATE OR ALTER PROCEDURE dt._CompleteTasks
    @CompletedTasks MessageIDs READONLY,
    @Results TaskEvents READONLY
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    BEGIN TRANSACTION

    /* Ensure the instance exists and is running before attempting to handle task results.
       We need to do this first and hold the lock to avoid race conditions and deadlocks with other operations. */
    DECLARE @existingInstanceID varchar(100)

    SELECT @existingInstanceID = R.[InstanceID]
    FROM Instances I WITH (HOLDLOCK)
        INNER JOIN @Results R ON 
            I.[TaskHub] = @TaskHub AND
            I.[InstanceID] = R.[InstanceID] AND 
            I.[ExecutionID] = R.[ExecutionID] AND
            I.[RuntimeStatus] IN ('Running', 'Suspended')
    
    -- If we find the instance, save the result to the [NewEvents] table.
    IF @existingInstanceID IS NOT NULL
    BEGIN
        -- Insert new event data payloads into the Payloads table in batches.
        -- PayloadID values are provided by the caller only if a payload exists.
        INSERT INTO Payloads ([TaskHub], [InstanceID], [PayloadID], [Text], [Reason])
            SELECT @TaskHub, [InstanceID], [PayloadID], [PayloadText], [Reason]
            FROM @Results
            WHERE [PayloadID] IS NOT NULL

        INSERT INTO NewEvents (
            [TaskHub],
            [InstanceID],
            [ExecutionID],
            [Name],
            [EventType],
            [TaskID],
            [VisibleTime],
            [PayloadID]
        ) 
        SELECT
            @TaskHub,
            R.[InstanceID],
            R.[ExecutionID],
            R.[Name],
            R.[EventType],
            R.[TaskID],
            R.[VisibleTime],
            R.[PayloadID]
        FROM @Results R
    END

    DECLARE @payloadsToDelete TABLE ([PayloadID] uniqueidentifier NULL)

    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE N
    OUTPUT DELETED.[PayloadID] INTO @payloadsToDelete
    OUTPUT DELETED.[SequenceNumber]
    FROM NewTasks N WITH (FORCESEEK(PK_NewTasks(TaskHub, SequenceNumber)))
        INNER JOIN @CompletedTasks C ON
            C.[SequenceNumber] = N.[SequenceNumber] AND
            N.[TaskHub] = @TaskHub

    -- If we fail to delete the messages then we must abort the transaction.
    -- This can happen if the message was completed by another worker, in which
    -- case we don't want any of the side-effects to persist.
    IF @@ROWCOUNT <> (SELECT COUNT(*) FROM @CompletedTasks)
        THROW 50002, N'Failed to delete the completed task events(s). They may have been deleted by another worker, in which case the current execution is likely a duplicate. Any results or pending side-effects of this task activity execution will be discarded.', 1;

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._GetVersions
AS
BEGIN
    SELECT SemanticVersion, UpgradeTime
    FROM Versions
    ORDER BY UpgradeTime DESC
END
GO


CREATE OR ALTER PROCEDURE dt._UpdateVersion
    @SemanticVersion varchar(100)
AS
BEGIN
    -- Duplicates are ignored (per the schema definition of dt.Versions)
    INSERT INTO Versions (SemanticVersion)
    VALUES (@SemanticVersion)
END
GO


CREATE OR ALTER PROCEDURE dt._RewindInstance
    @InstanceID varchar(100),
    @Reason varchar(max) = NULL
AS
BEGIN
    BEGIN TRANSACTION

    EXEC dt._RewindInstanceRecursive @InstanceID, @Reason

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE dt._RewindInstanceRecursive
    @InstanceID varchar(100),
    @Reason varchar(max) = NULL
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Instances --> (History --> Payloads --> History), NewEvents)  

    DECLARE @existingStatus varchar(30)
    DECLARE @executionID varchar(50)
    
    SELECT TOP 1 @existingStatus = existing.[RuntimeStatus], @executionID = existing.[ExecutionID]
    FROM Instances existing WITH (HOLDLOCK)
    WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID

    -- Instance IDs can be overwritten only if the orchestration is in a terminal state
    IF @existingStatus NOT IN ('Failed')
    BEGIN
        DECLARE @msg nvarchar(4000) = FORMATMESSAGE('Cannot rewing instance with ID ''%s'' because it is not in a ''Failed'' state, but in ''%s'' state.', @InstanceID, @existingStatus);
        THROW 50001, @msg, 1;
    END
    
    DECLARE @eventsInFailure TABLE (
        [SequenceNumber] bigint NULL,
        [EventType] varchar(40) NULL,
        [TaskID] int NULL,
        [DataPayloadID] uniqueidentifier NULL)
    
    -- List all events related to failures (ie TaskScheduled/TaskFailed and SubOrchestrationInstanceStarted/SubOrchestrationInstanceFailed couples)
    INSERT INTO @eventsInFailure
    SELECT h.[SequenceNumber], h.[EventType], h.[TaskID], h.[DataPayloadID]
    FROM History h
    WHERE h.[TaskHub] = @TaskHub AND h.[InstanceID] = @InstanceID
      AND (h.[EventType] IN ('TaskFailed', 'SubOrchestrationInstanceFailed')
           OR (h.[EventType] IN ('TaskScheduled', 'SubOrchestrationInstanceStarted') AND EXISTS (SELECT 1
                                                           FROM History f
                                                           WHERE f.[TaskHub] = @TaskHub AND f.[InstanceID] = @InstanceID  AND f.[TaskID] = h.[TaskID] AND f.[EventType] = CASE WHEN h.[EventType] = 'TaskScheduled' THEN 'TaskFailed' ELSE 'SubOrchestrationInstanceFailed' END)))

    -- Mark all events related to failure as rewound
    UPDATE Payloads
    SET [Reason] = CONCAT('Rewound: ', ef.[EventType])
    FROM Payloads p
    JOIN @eventsInFailure ef ON p.[PayloadID] = ef.[DataPayloadID]
    WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID AND [SequenceNumber] IN (SELECT [SequenceNumber] FROM @eventsInFailure WHERE [DataPayloadID] IS NOT NULL AND [EventType] IN ('TaskScheduled', 'SubOrchestrationInstanceCreated'))

    DECLARE @sequenceNumber bigint
    DECLARE @eventType varchar(40)
    DECLARE @payloadId uniqueidentifier
    DECLARE sequenceNumberCursor CURSOR LOCAL FOR
        SELECT [SequenceNumber], [EventType]
        FROM @eventsInFailure
        WHERE [DataPayloadID] IS NULL

    OPEN sequenceNumberCursor 
    FETCH NEXT FROM sequenceNumberCursor INTO @sequenceNumber, @eventType

    WHILE @@FETCH_STATUS = 0 BEGIN
        SET @payloadId = NEWID()
        INSERT INTO Payloads (
            [TaskHub],
            [InstanceID],
            [PayloadID],
            [Reason]
        )
        VALUES (@TaskHub, @InstanceID, @payloadId, CONCAT('Rewound: ', @eventType))
        FETCH NEXT FROM sequenceNumberCursor INTO @sequenceNumber, @eventType
    END
    CLOSE sequenceNumberCursor
    DEALLOCATE sequenceNumberCursor

    -- Transform all events related to failure to GenericEvents, except for SubOrchestrationInstanceStarted that can be kept
    UPDATE History
    SET [EventType] = 'GenericEvent'
    WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
      AND ([SequenceNumber] IN (SELECT [SequenceNumber]
                                FROM @eventsInFailure WHERE [EventType] <> 'SubOrchestrationInstanceStarted')
           OR [RuntimeStatus] = 'Failed')

    -- Enumerate instances of sub orchestrations related to SubOrchestrationInstanceFailed events and rewing them recursively
    DECLARE @subOrchestrationInstanceID varchar(100)
    DECLARE subOrchestrationCursor CURSOR LOCAL FOR
        SELECT i.[InstanceID]
        FROM dt.Instances i
          JOIN dt.History h ON i.[TaskHub] = h.[TaskHub] AND i.[InstanceID] = h.[InstanceID]
          JOIN @eventsInFailure e ON e.[TaskID] = h.[TaskID]
        WHERE i.[ParentInstanceID] = @InstanceID 
          AND h.[EventType] = 'ExecutionStarted'
          AND e.[EventType] = 'SubOrchestrationInstanceFailed'


    OPEN subOrchestrationCursor 
    FETCH NEXT FROM subOrchestrationCursor INTO @subOrchestrationInstanceID

    WHILE @@FETCH_STATUS = 0 BEGIN
        -- Call rewind recursively on the failing suborchestrations
        EXECUTE dt._RewindInstanceRecursive @subOrchestrationInstanceID, @Reason
        FETCH NEXT FROM subOrchestrationCursor INTO @subOrchestrationInstanceID
    END
    CLOSE subOrchestrationCursor
    DEALLOCATE subOrchestrationCursor

    -- Insert a line in NewEvents to ensure orchestration will start
    SET @payloadId = NEWID()
    INSERT INTO Payloads (
        [TaskHub],
        [InstanceID],
        [PayloadID],
        [Text]
    )
    VALUES (@TaskHub, @InstanceID, @payloadId, @reason)
    INSERT INTO NewEvents (
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [EventType],
        [PayloadID]
    ) 
    VALUES(
        @TaskHub,
        @InstanceID,
        @executionID,
        'GenericEvent',
        @payloadId)

    -- Set orchestration status to Pending
    UPDATE Instances
    SET [RuntimeStatus] = 'Pending', [LastUpdatedTime] = SYSUTCDATETIME()
    WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
END
GO