namespace DurableTask.SqlServer.Tests.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    public class LogEntry
    {
        public LogEntry(string category, LogLevel level, EventId eventId, string message)
        {
            this.Category = category;
            this.LogLevel = level;
            this.EventId = eventId;
            this.Message = message;
            this.Timestamp = DateTime.Now;
        }

        public string Category { get; }

        public DateTime Timestamp { get; }

        public EventId EventId { get; }

        public LogLevel LogLevel { get; }

        public string Message { get; }

        public override string ToString()
        {
            return $"{this.Timestamp:o} [{this.Category}] {this.Message}";
        }
    }
}
