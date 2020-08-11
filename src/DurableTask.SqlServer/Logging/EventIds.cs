namespace DurableTask.SqlServer.Logging
{
    // WARNING: Changing the *name* OR the *value* of any of these constants is a breaking change!!
    /// <summary>
    /// List of logging event IDs supported by this provider.
    /// </summary>
    static class EventIds
    {
        public const int AcquiredAppLock = 300;
        public const int ExecutedSqlScript = 301;
        public const int SprocCompleted = 302;
        public const int ProcessingFailure = 303;
        public const int SchedulingLocalActivity = 304;
        public const int StartingLocalActivity = 305;
        public const int CheckpointingOrchestration = 306;
    }
}
