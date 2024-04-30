// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinePersistentCache.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Xunit.Abstractions;

    class TestCheckpoint : CheckpointCommand
    {        
        public long? Seqno;
        public List<Action> Actions = new();
        public Dictionary<(string,string), (string? value, Writeback writeback)> Deltas = new();

        public override void AddPostCheckpointActions(IEnumerable<Action> actions)
        {
            this.Actions.AddRange(actions);
        }

        public override void SetSequenceCounter(long sequenceCounter)
        {
            this.Seqno = sequenceCounter;
        }

        public void AddDelta(string table, string key, string? value, Writeback writeback)
        {
            if (this.Deltas.ContainsKey((table,key)))
            {
                throw new Exception("more than one delta per key");
            }
            if (writeback == Writeback.None)
            {
                throw new Exception("invalid writeback: none");
            }
            if (writeback != Writeback.Deleted && value == null)
            {
                throw new Exception("value must be provided for created or updated");
            }

            this.Deltas.Add((table, key), (value, writeback));

            Console.WriteLine($"Delta: {table}.{key} = {value} {writeback}");
        }

        public IEnumerable<(string key, string? value, Writeback writeback)> GetDeltas(string table)
        {
            return this.Deltas
               .Where(kvp => kvp.Key.Item1 == table)
               .Select(kvp => (kvp.Key.Item2, kvp.Value.value, kvp.Value.writeback))
               .OrderBy(tuple => tuple.Item1)
               .ToList();
        }
    }
}
