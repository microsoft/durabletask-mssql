// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    /// <summary>
    /// Represents a row that is being tracked for changes, within transactions, and since the last checkpoint.
    /// </summary>
    abstract class TrackedRow
    {
        public abstract void AddChangesToCheckpointCommand(object command);
    
        public abstract void DiscardRollbackInformation();

        public abstract void Rollback();
    }
}
