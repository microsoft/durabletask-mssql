// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
    using DurableTask.Core;

    struct DbTaskEvent
    {
        readonly DateTime timestamp;

        public DbTaskEvent(TaskMessage message, int taskId, string? lockedBy = null, DateTime? lockExpiration = null)
        {
            this.Message = message ?? throw new ArgumentNullException(nameof(message));
            this.TaskId = taskId;
            this.LockedBy = lockedBy;
            this.LockExpiration = lockExpiration;
            this.timestamp = DateTime.Now;
        }

        public TaskMessage Message { get; }

        public int TaskId { get; }

        public string? LockedBy { get; }

        public DateTime? LockExpiration { get; }

        public bool IsLocal => this.LockedBy != null;

        public TimeSpan GetAge() => DateTime.Now.Subtract(this.timestamp);
    }
}
