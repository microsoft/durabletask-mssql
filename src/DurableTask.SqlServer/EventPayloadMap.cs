// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using DurableTask.Core.History;

    class EventPayloadMap
    {
        readonly Dictionary<(EventType, int), Guid> payloadIdsByEventId;
        readonly Dictionary<HistoryEvent, Guid> payloadIdsByEventReference;

        readonly byte[] timestamp = BitConverter.GetBytes(DateTime.UtcNow.Ticks);
        short sequenceNumber;

        public EventPayloadMap(int capacity)
        {
            this.payloadIdsByEventId = new Dictionary<(EventType, int), Guid>(capacity);
            this.payloadIdsByEventReference = new Dictionary<HistoryEvent, Guid>(capacity);
        }

        public void Add(HistoryEvent e, Guid payloadId)
        {
            if (CanTrackByReference(e))
            {
                this.payloadIdsByEventReference.Add(e, payloadId);
            }
            else
            {
                var key = ValueTuple.Create(e.EventType, DTUtils.GetTaskEventId(e));
                this.payloadIdsByEventId.Add(key, payloadId);
            }
        }

        public void Add(IList<TaskMessage> outboundMessages)
        {
            for (int i = 0; i < outboundMessages.Count; i++)
            {
                HistoryEvent e = outboundMessages[i].Event;
                if (DTUtils.HasPayload(e))
                {
                    this.Add(e, this.NewPayloadId(e));
                }
            }
        }

        public bool TryGetPayloadId(HistoryEvent e, out Guid payloadId)
        {
            if (CanTrackByReference(e))
            {
                return this.payloadIdsByEventReference.TryGetValue(e, out payloadId);
            }
            else
            {
                var key = ValueTuple.Create(e.EventType, DTUtils.GetTaskEventId(e));
                return this.payloadIdsByEventId.TryGetValue(key, out payloadId);
            }
        }

        static bool CanTrackByReference(HistoryEvent e)
        {
            // DTFx sometimes creates different object references between messages and history events, which
            // means we have to use some other mechanism for tracking.
            return e.EventType != EventType.TaskScheduled && e.EventType != EventType.SubOrchestrationInstanceCreated;
        }

        Guid NewPayloadId(HistoryEvent e)
        {
            // Sequential GUIDs are simply to make reading slightly easier. They don't have any other purpose.
            // Example: 00000001-0004-0000-ca2b-694a052ada08
            return new Guid(DTUtils.GetTaskEventId(e), (short)e.EventType, this.sequenceNumber++, this.timestamp);
        }
    }
}
