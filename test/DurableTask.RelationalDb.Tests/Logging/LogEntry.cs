namespace DurableTask.RelationalDb.Tests.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    class LogEntry
    {
        public LogEntry(LogLevel level, string message)
        {
            this.LogLevel = level;
            this.Message = message;
            this.Timestamp = DateTime.Now;
        }

        public DateTime Timestamp { get; }

        public LogLevel LogLevel { get; }

        public string Message { get; }

        public override string ToString()
        {
            return $"{this.Timestamp:o}: {this.Message}";
        }
    }
}
