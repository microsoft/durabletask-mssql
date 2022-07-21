-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- Security 
IF DATABASE_PRINCIPAL_ID('{{SchemaNamePlaceholder}}_runtime') IS NULL
BEGIN
    -- This is the role to which all low-privilege user accounts should be associated using
    -- the 'ALTER ROLE dt_runtime ADD MEMBER [<username>]' statement.
    CREATE ROLE {{SchemaNamePlaceholder}}_runtime
END

-- Each stored procedure that is granted to dt_runtime must limits access to data based 
-- on the task hub since that is. that no
-- database user can access data created by another database user.

-- Functions 
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.GetScaleMetric TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.GetScaleRecommendation TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.CurrentTaskHub TO {{SchemaNamePlaceholder}}_runtime

-- Public sprocs
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.CreateInstance TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.GetInstanceHistory TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.QuerySingleOrchestration TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.RaiseEvent TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.TerminateInstance TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.PurgeInstanceStateByID TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}.PurgeInstanceStateByTime TO {{SchemaNamePlaceholder}}_runtime

-- Internal sprocs
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._AddOrchestrationEvents TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._CheckpointOrchestration TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._CompleteTasks TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._DiscardEventsAndUnlockInstance TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._GetVersions TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._LockNextOrchestration TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._LockNextTask TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._QueryManyOrchestrations TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._RenewOrchestrationLocks TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._RenewTaskLocks TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._UpdateVersion TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._RewindInstance TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON OBJECT::{{SchemaNamePlaceholder}}._RewindInstanceRecursive TO {{SchemaNamePlaceholder}}_runtime

-- Types
GRANT EXECUTE ON TYPE::{{SchemaNamePlaceholder}}.HistoryEvents TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON TYPE::{{SchemaNamePlaceholder}}.MessageIDs TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON TYPE::{{SchemaNamePlaceholder}}.InstanceIDs TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON TYPE::{{SchemaNamePlaceholder}}.OrchestrationEvents TO {{SchemaNamePlaceholder}}_runtime
GRANT EXECUTE ON TYPE::{{SchemaNamePlaceholder}}.TaskEvents TO {{SchemaNamePlaceholder}}_runtime

GO