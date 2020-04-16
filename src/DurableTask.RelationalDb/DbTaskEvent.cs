namespace DurableTask.RelationalDb
{
    using System;
    using DurableTask.Core;

    struct DbTaskEvent
    {
        public DbTaskEvent(TaskMessage message, int taskId, string? lockedBy = null, DateTime? lockExpiration = null)
        {
            this.Message = message ?? throw new ArgumentNullException(nameof(message));
            this.TaskId = taskId;
            this.LockedBy = lockedBy;
            this.LockExpiration = lockExpiration;
        }

        public TaskMessage Message { get; }

        public int TaskId { get; }

        public string? LockedBy { get; }

        public DateTime? LockExpiration { get; }

        public bool IsLocal => this.LockedBy != null;
    }
}
