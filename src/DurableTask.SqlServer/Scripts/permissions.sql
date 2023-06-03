-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

-- Security 
IF DATABASE_PRINCIPAL_ID('__SchemaNamePlaceholder___runtime') IS NULL
BEGIN
    -- This is the role to which all low-privilege user accounts should be associated using
    -- the 'ALTER ROLE dt_runtime ADD MEMBER [<username>]' statement.
    CREATE ROLE __SchemaNamePlaceholder___runtime
END

-- Each stored procedure that is granted to dt_runtime must limits access to data based 
-- on the task hub since that is. that no
-- database user can access data created by another database user.

-- Functions 
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._GetScaleMetric TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.GetScaleMetric TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._GetScaleRecommendation TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.GetScaleRecommendation TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._CurrentTaskHub TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.CurrentTaskHub TO __SchemaNamePlaceholder___runtime

-- Public sprocs
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.CreateInstance TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.GetInstanceHistory TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.QuerySingleOrchestration TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.RaiseEvent TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.TerminateInstance TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.PurgeInstanceStateByID TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__.PurgeInstanceStateByTime TO __SchemaNamePlaceholder___runtime

-- Internal sprocs
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._AddOrchestrationEvents TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._CheckpointOrchestration TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._CompleteTasks TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._DiscardEventsAndUnlockInstance TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._GetVersions TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._LockNextOrchestration TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._LockNextTask TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._QueryManyOrchestrations TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._RenewOrchestrationLocks TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._RenewTaskLocks TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._UpdateVersion TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._RewindInstance TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON OBJECT::__SchemaNamePlaceholder__._RewindInstanceRecursive TO __SchemaNamePlaceholder___runtime

-- Types
GRANT EXECUTE ON TYPE::__SchemaNamePlaceholder__.HistoryEvents TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON TYPE::__SchemaNamePlaceholder__.MessageIDs TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON TYPE::__SchemaNamePlaceholder__.InstanceIDs TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON TYPE::__SchemaNamePlaceholder__.OrchestrationEvents TO __SchemaNamePlaceholder___runtime
GRANT EXECUTE ON TYPE::__SchemaNamePlaceholder__.TaskEvents TO __SchemaNamePlaceholder___runtime

GO