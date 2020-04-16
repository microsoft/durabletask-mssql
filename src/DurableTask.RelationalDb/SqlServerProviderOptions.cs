namespace DurableTask.RelationalDb
{
    using System;
    using Microsoft.Extensions.Logging;

    public class SqlServerProviderOptions
    {
        public string ConnectionString { get; set; } = "Server=localhost;Database=TaskHub;Trusted_Connection=True;";

        public int MaxActivityConcurrency { get; set; } = Environment.ProcessorCount;

        public int MaxOrchestrationConcurrency { get; set; } = Environment.ProcessorCount;

        public ILoggerFactory LoggerFactory { get; set; } = new LoggerFactory();

        public TimeSpan TaskEventLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        public string AppName { get; set; } = Environment.MachineName;
    }
}
