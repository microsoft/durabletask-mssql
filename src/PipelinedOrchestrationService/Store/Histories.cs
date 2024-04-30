// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PipelinedOrchestrationService
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using PipelinePersistentCache;

    class Histories : Table<string, HistoryEvent[], SqlCheckpointCommand>
    {
        readonly SqlStore store;

        public Histories(SqlStore store)
        {
            this.store = store;
        }

        public void EnsureInMemory(TxContext context, string instanceId)
        {
            base.PrefetchRow(context, instanceId);
        }

        public HistoryEvent[] GetHistory(TxContext context, string instanceId)
        {
            if (base.TryGetRow(context, instanceId, out var events))
            {
                return events!;
            }
            else
            {
                throw new InvalidOperationException("history does not exist");
            }
        }

        public void SetHistory(TxContext context, string instanceId, IList<HistoryEvent> events)
        {
            base.UpdateExistingRow(context, instanceId, events.ToArray());
        }

        public void AppendHistory(TxContext context, string instanceId, int position, IList<HistoryEvent> events)
        {
            bool exists = base.TryGetRow(context, instanceId, out HistoryEvent[]? current);
            Debug.Assert(exists && current!.Length == position);
            var combined = new HistoryEvent[position + events.Count];
            for(int i = 0; i < position; i++)
            {
                combined[i] = current![i];
            }
            for (int i = 0; i < events.Count; i++)
            {
                combined[position + i] = events[i];
            }
            base.UpdateExistingRow(context, instanceId, combined);
        }

        public void DeleteHistory( TxContext context, string instanceId)
        {
            base.DeleteExistingRow(context, instanceId);
        }

        protected override Task<(bool exists, HistoryEvent[]? value)> LoadAsync(string key)
        {
            //TODO
            throw new NotImplementedException();
        }

        protected override void AddDeltaToCheckpointCommand(SqlCheckpointCommand command, Writeback writeback, string key, HistoryEvent[]? Current)
        {
            //TODO
            throw new NotImplementedException();
        }
    }
}
