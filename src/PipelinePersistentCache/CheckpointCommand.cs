// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Abstractly represents an implementation of a command that writes back a batch of changes (a checkpoint) to the underlying store.
    /// A checkpoint can modify one or more owned partitions. Concurrent checkpoints will never conflict unless they modify the same partition, 
    /// which does not happend under normal operation (because partitions are uniquely owned by service hosts). However, in rare split-brain scenarios,
    /// if two service hosts think they own the same partitions, checkpoints may indeed conflict - and in fact, are *guaranteed* to conflict because they both
    /// write to the same partition, triggering a write-write conflict in the underlying store. This makes it possible to recover correctly from split-brain scenarios.
    /// </summary>
    public abstract class CheckpointCommand
    {
        /// <summary>
        /// How to update the partition meta data for this checkpoint.
        /// </summary>
        /// <param name="sequenceCounter"></param>
        public abstract void SetPartitionMetaData(PartitionMetaData partitionMetaData);

        /// <summary>
        /// Adds an action that should be performed after the checkpoint is persisted.
        /// </summary>
        /// <param name="actions"></param>
        public abstract void AddPostCheckpointAction(Action actions);
    }
}