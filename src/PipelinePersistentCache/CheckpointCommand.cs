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
    /// </summary>
    public abstract class CheckpointCommand
    {
        /// <summary>
        /// The sequence counter to be persisted for this checkpoint. The sequence counter is monotonically increasing and can be used to compare the order of two checkpoints.
        /// </summary>
        /// <param name="sequenceCounter"></param>
        public abstract void SetSequenceCounter(long sequenceCounter);

        /// <summary>
        /// Adds actions that should be performed after the checkpoint is persisted.
        /// </summary>
        /// <param name="actions"></param>
        public abstract void AddPostCheckpointActions(IEnumerable<Action> actions);
    }
}