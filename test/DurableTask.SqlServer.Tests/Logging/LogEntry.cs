namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    class LogEntry
    {
        public LogEntry(LogLevel level, EventId eventId, string message)
        {
            this.LogLevel = level;
            this.EventId = eventId;
            this.Message = message;
            this.Timestamp = DateTime.Now;
        }

        public DateTime Timestamp { get; }

        public EventId EventId { get; }

        public LogLevel LogLevel { get; }

        public string Message { get; }

        public override string ToString()
        {
            return $"{this.Timestamp:o}: {this.Message}";
        }
    }
}
