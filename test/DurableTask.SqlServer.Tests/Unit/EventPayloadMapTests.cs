// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Xunit;

    public class EventPayloadMapTests
    {
        [Fact]
        public void PayloadIdsStayUniqueAcrossClear()
        {
            // Extended sessions reuse one map for every turn and clear it after each checkpoint.
            // Events sent to other instances (entity requests and responses, SendEvent) all have
            // EventId -1, so the generated ID must not repeat from one turn to the next.
            var map = new EventPayloadMap(capacity: 1);

            Guid firstTurnId = AddEventRaised(map);
            map.Clear();
            Guid secondTurnId = AddEventRaised(map);

            Assert.NotEqual(firstTurnId, secondTurnId);
        }

        [Fact]
        public void PayloadIdsStayUniqueWhenSequenceNumberWraps()
        {
            var map = new EventPayloadMap(capacity: 1);
            var ids = new HashSet<Guid>();

            // Enough IDs to wrap the 16-bit sequence number and keep going.
            for (int i = 0; i < ushort.MaxValue + 10; i++)
            {
                Assert.True(ids.Add(AddEventRaised(map)), $"Duplicate payload ID generated at iteration {i}.");
                map.Clear();
            }
        }

        static Guid AddEventRaised(EventPayloadMap map)
        {
            var message = new TaskMessage
            {
                Event = new EventRaisedEvent(-1, "\"payload\"") { Name = "op" },
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "@counter@1" },
            };

            map.Add(new List<TaskMessage> { message });
            Assert.True(map.TryGetPayloadId(message.Event, out Guid payloadId));
            return payloadId;
        }
    }
}
