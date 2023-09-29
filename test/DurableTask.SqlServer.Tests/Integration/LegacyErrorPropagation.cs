// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Utils;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    public class LegacyErrorPropagation : IAsyncLifetime
    {
        readonly TestService testService;

        public LegacyErrorPropagation(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync(legacyErrorPropagation: true);

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task UncaughtOrchestrationException()
        {
            string errorMessage = "Kah-BOOOOOM!!!";

            // The exception is expected to fail the orchestration execution
            TestInstance<string> instance = await this.testService.RunOrchestration<string, string>(
                null,
                orchestrationName: "OrchestrationWithException",
                implementation: (ctx, input) => throw new ApplicationException(errorMessage));

            await instance.WaitForCompletion(expectedOutput: errorMessage, expectedStatus: OrchestrationStatus.Failed);
        }

        [Fact]
        public async Task UncaughtActivityException()
        {
            var exceptionToThrow = new ApplicationException("Kah-BOOOOOM!!!");

            // Schedules a task that throws an uncaught exception
            TestInstance<string> instance = await this.testService.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: (ctx, input) => ctx.ScheduleTask<string>("Throw", ""),
                activities: new[] {
                    ("Throw", TestService.MakeActivity<string, string>((ctx, input) => throw exceptionToThrow)),
                });

            OrchestrationState state = await instance.WaitForCompletion(expectedStatus: OrchestrationStatus.Failed);
            Assert.Equal(exceptionToThrow.Message, state.Output);
        }

        [Fact]
        public async Task CatchActivityException()
        {
            var innerException = new InvalidOperationException("Oops");
            var exceptionToThrow = new ApplicationException("Kah-BOOOOOM!!!", innerException);

            // Schedules a task that throws an exception, which is then caught by the orchestration
            TestInstance<string> instance = await this.testService.RunOrchestration(
                null as string,
                orchestrationName: "OrchestrationWithActivityFailure",
                implementation: async (ctx, input) =>
                {
                    try
                    {
                        await ctx.ScheduleTask<string>("Throw", "");
                        return null; // not expected
                    }
                    catch (TaskFailedException e)
                    {
                        return e.InnerException;
                    }
                },
                activities: new[] {
                    ("Throw", TestService.MakeActivity<string, string>((ctx, input) => throw exceptionToThrow)),
                });

            OrchestrationState state = await instance.WaitForCompletion();

            Assert.NotNull(state.Output);

            // The output should be a serialized ApplicationException.
            // NOTE: Need to specify TypeNameHandling.All to get the exception types to be honored.
            Exception caughtException = JsonConvert.DeserializeObject<Exception>(
                state.Output,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All });
            Assert.NotNull(caughtException);
            Assert.Equal(exceptionToThrow.Message, caughtException.Message);
            Assert.IsType<ApplicationException>(caughtException);

            // Check that the inner exception was correctly preserved.
            Assert.NotNull(caughtException.InnerException);
            Exception caughtInnerException = Assert.IsType<InvalidOperationException>(caughtException.InnerException);
            Assert.Equal(innerException.Message, caughtInnerException.Message);
        }
    }
}
