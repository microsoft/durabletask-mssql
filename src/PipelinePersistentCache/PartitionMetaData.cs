// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Metadata about a partition. This is saved to and recovered from storage with each checkpoint.
    /// </summary>
    public record PartitionMetaData(int PartitionId, long LastCheckpointId, long[] DeduplicationVector)
    {
        public static PartitionMetaData GetInitialState(int partitionId, int totalPartitions)
        {
            return new PartitionMetaData(partitionId, 0, new long[totalPartitions]);
        }
    }
}
