namespace DurableTask.SqlServer.Logging
{
    /// <summary>
    /// List of logging event IDs supported by this provider.
    /// </summary>
    static class EventIds
    {
        // WARNING: Changing the *name* OR the *value* of any of these constants is a breaking change!!
        public const int AcquiredAppLock = 300;
        public const int ExecutedSqlScript = 301;
        public const int SprocCompleted = 302;
        public const int ProcessingFailure = 303;
        public const int GenericWarning = 304;
        public const int CheckpointStarting = 305;
        public const int CheckpointCompleted = 306;
        public const int DuplicateExecutionDetected = 307;
    }
}
