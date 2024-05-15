// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    static class PartitionHash
    {
        public static int GetPartitionId(string instanceId, uint totalPartitions)
        {
            int placementSeparatorPosition = instanceId.LastIndexOf('!');

            // if the instance id ends with !nnn, where nnn is an unsigned number, it indicates explicit partition placement
            if (placementSeparatorPosition != -1
                && uint.TryParse(instanceId.Substring(placementSeparatorPosition + 1), out uint index))
            {
                var partitionId = index % totalPartitions;
                return (int) partitionId;
            }
            else
            {
                return (int) (ComputeHash(instanceId) % totalPartitions);
            }
        }

        const uint FnvPrime = unchecked(16777619);
        const uint FnvOffsetBasis = unchecked(2166136261);

        static uint ComputeHash(string value)
        {
            return ComputeHash(value, encoding: null);
        }

        static uint ComputeHash(string value, Encoding? encoding)
        {
            return ComputeHash(value, encoding, hash: FnvOffsetBasis);
        }

        static uint ComputeHash(string value, Encoding? encoding, uint hash)
        {
            byte[] bytes = (encoding ?? Encoding.UTF8).GetBytes(value);
            return ComputeHash(bytes, hash);
        }

        static uint ComputeHash(byte[] array)
        {
            return ComputeHash(array, hash: FnvOffsetBasis);
        }

        static uint ComputeHash(byte[] array, uint hash)
        {
            for (var i = 0; i < array.Length; i++)
            {
                unchecked
                {
                    hash ^= array[i];
                    hash *= FnvPrime;
                }
            }

            return hash;
        }
    }
}
