CREATE OR ALTER FUNCTION dt.CurrentTaskHub()
    RETURNS varchar(50)
    WITH EXECUTE AS CALLER
AS
BEGIN
    -- Implemented as a function to make it easier to customize
    DECLARE @TaskHub varchar(50)
    IF LEN(CURRENT_USER) <= 50
        -- default behavior is to just use the username
        SET @TaskHub = CONVERT(varchar(50), CURRENT_USER)
    ELSE
        -- if the username is too long, keep the first 16 characters and hash the rest
        SET @TaskHub = CONVERT(varchar(16), CURRENT_USER) + '__' + CONVERT(varchar(32), HashBytes('MD5', CURRENT_USER), 2)

    RETURN @TaskHub
END
GO


CREATE OR ALTER VIEW dt.vInstances
AS
    SELECT
        I.[TaskHub],
        I.[InstanceID],
        I.[ExecutionID],
        I.[Name],
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
        DECLARE @existingStatus varchar(30) = (
            SELECT TOP 1 existing.[RuntimeStatus]
            FROM Instances existing WITH (HOLDLOCK)
            WHERE [TaskHub] = @TaskHub AND [InstanceID] = @InstanceID
        )

        -- TODO: Allow overwriting if the existing status is a non-running status
        -- REVIEW: Do we need to explicitly rollback the transaction or is this automatic?
        IF @existingStatus IS NOT NULL
            THROW 50001, N'An instance with this ID already exists.', 1;
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
        [TaskHub],
        [InstanceID],
        [ExecutionID],
        [RuntimeStatus],
        [InputPayloadID])
    VALUES (
        @Name,
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
        [PayloadID]
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
            [RuntimeStatus])
        SELECT
            @TaskHub,
            @InstanceID,
            NEWID(),
            SUBSTRING(@InstanceID, 2, CHARINDEX('@', @InstanceID, 2) - 2),
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


CREATE OR ALTER PROCEDURE dt.PurgeInstanceState
    @ThresholdTime datetime2,
    @FilterType tinyint = 0
AS
BEGIN
    DECLARE @TaskHub varchar(50) = dt.CurrentTaskHub()

    DECLARE @InstanceIDs TABLE (InstanceID varchar(100))

    IF @FilterType = 0 -- created time
    BEGIN
        INSERT INTO @InstanceIDs
            SELECT [InstanceID] FROM Instances
            WHERE [TaskHub] = @TaskHub AND [RuntimeStatus] IN ('Completed', 'Terminated', 'Failed')
                AND [CreatedTime] >= @ThresholdTime
    END
    ELSE IF @FilterType = 1 -- completed time
    BEGIN
        INSERT INTO @InstanceIDs
            SELECT [InstanceID] FROM Instances
            WHERE [TaskHub] = @TaskHub AND [RuntimeStatus] IN ('Completed', 'Terminated', 'Failed')
                AND [CompletedTime] >= @ThresholdTime
    END
    ELSE
    BEGIN
        DECLARE @msg nvarchar(100) = FORMATMESSAGE('Unknown or unsupported filter type: %d', @FilterType);
        THROW 50000, @msg, 1;
    END

    BEGIN TRANSACTION

    DELETE FROM NewEvents WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DELETE FROM Instances WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    DELETE FROM Payloads WHERE [TaskHub] = @TaskHub AND [InstanceID] IN (SELECT [InstanceID] FROM @InstanceIDs)
    -- Other relevant tables are expected to be cleaned up via cascade deletes

    COMMIT TRANSACTION

    -- return the number of deleted instances
    RETURN (SELECT COUNT(*) FROM @InstanceIDs)
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
        @instanceID = I.[InstanceID]
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
        DATEDIFF(millisecond, [Timestamp], @now) AS [WaitTime]
    FROM NewEvents N
        LEFT OUTER JOIN Payloads P ON 
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = N.[InstanceID] AND
            P.[PayloadID] = N.[PayloadID]
    WHERE N.[TaskHub] = @TaskHub AND N.[InstanceID] = @instanceID

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
        [PayloadID]
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
        [RuntimeStatus])
    SELECT DISTINCT
        @TaskHub,
        E.[InstanceID],
        NEWID(),
        SUBSTRING(E.[InstanceID], 2, CHARINDEX('@', E.[InstanceID], 2) - 2),
        'Pending'
    FROM @NewOrchestrationEvents E
    WHERE NOT EXISTS (
        SELECT 1
        FROM dt.Instances I
        WHERE [TaskHub] = @TaskHub AND I.[InstanceID] = E.[InstanceID])
    GROUP BY E.[InstanceID]
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
        [PayloadID]
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
        [PayloadID]
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
            [RuntimeStatus])
        SELECT DISTINCT
            @TaskHub,
            E.[InstanceID],
            NEWID(),
            SUBSTRING(E.[InstanceID], 2, CHARINDEX('@', E.[InstanceID], 2) - 2),
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
        I.[CreatedTime],
        I.[LastUpdatedTime],
        I.[CompletedTime],
        I.[RuntimeStatus],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[CustomStatusPayloadID]) AS [CustomStatusText],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            @FetchInput = 1 AND 
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[InputPayloadID]) AS [InputText],
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            @FetchOutput = 1 AND 
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = I.[InstanceID] AND
            P.[PayloadID] = I.[OutputPayloadID]) AS [OutputText]
    FROM Instances I
    WHERE
        I.[TaskHub] = @TaskHub AND
        I.[InstanceID] = @InstanceID AND
        (@ExecutionID IS NULL OR @ExecutionID = I.ExecutionID)
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
        (SELECT TOP 1 [Text] FROM Payloads P WHERE
            P.[TaskHub] = @TaskHub AND
            P.[InstanceID] = N.[InstanceID] AND
            P.[PayloadID] = N.[PayloadID]) AS [PayloadText],
        DATEDIFF(millisecond, [Timestamp], @now) AS [WaitTime]
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
    -- *** IMPORTANT ***
    -- To prevent deadlocks, it is important to maintain consistent table access
    -- order across all stored procedures that execute within a transaction.
    -- Table order for this sproc: Payloads --> Instances --> NewEvents --> NewTasks

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
        -- This join ensures we don't add messages if the associated instance was completed, terminated, or doesn't exist
        INNER JOIN Instances I ON 
            I.[TaskHub] = @TaskHub AND
            I.[InstanceID] = R.[InstanceID] AND 
            I.[ExecutionID] = R.[ExecutionID] AND
            I.[RuntimeStatus] IN ('Running', 'Suspended')

    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE N
    OUTPUT DELETED.[SequenceNumber]
    FROM NewTasks N INNER JOIN @CompletedTasks C ON
        C.[SequenceNumber] = N.[SequenceNumber] AND
        N.[TaskHub] = @TaskHub

    -- If we fail to delete the messages then we must abort the transaction.
    -- This can happen if the message was completed by another worker, in which
    -- case we don't want any of the side-effects to persist.
    IF @@ROWCOUNT <> (SELECT COUNT(*) FROM @CompletedTasks)
        THROW 50002, N'Failed to delete the completed task events(s). They may have been deleted by another worker. Any pending side-effects will be discarded.', 1;

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
