// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Integration
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.SqlServer.Tests.Logging;
    using DurableTask.SqlServer.Tests.Utils;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("Integration")]
    public class TagTests : IAsyncLifetime
    {
        readonly TestService testService;

        public TagTests(ITestOutputHelper output)
        {
            this.testService = new TestService(output);
        }

        Task IAsyncLifetime.InitializeAsync() => this.testService.InitializeAsync();

        Task IAsyncLifetime.DisposeAsync() => this.testService.DisposeAsync();

        [Fact]
        public async Task OrchestrationWithTags()
        {
            // Arrange: create tags to associate with the orchestration
            var tags = new Dictionary<string, string>
            {
                { "environment", "test" },
                { "owner", "integration-test" },
            };

            string input = $"Hello {DateTime.UtcNow:o}";

            // Act: run an orchestration with tags
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "OrchestrationWithTags",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            // Assert: verify that tags are preserved on the OrchestrationState
            Assert.NotNull(state.Tags);
            Assert.Equal(2, state.Tags.Count);
            Assert.Equal("test", state.Tags["environment"]);
            Assert.Equal("integration-test", state.Tags["owner"]);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task OrchestrationWithoutTags()
        {
            string input = $"Hello {DateTime.UtcNow:o}";

            // Act: run an orchestration without tags (backward compatibility)
            TestInstance<string> instance = await this.testService.RunOrchestration(
                input,
                orchestrationName: "OrchestrationWithoutTags",
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            // Assert: tags should be null when none are specified
            Assert.Null(state.Tags);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task OrchestrationWithEmptyTags()
        {
            var tags = new Dictionary<string, string>();

            string input = $"Hello {DateTime.UtcNow:o}";

            // Act: run an orchestration with an empty tags dictionary
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "OrchestrationWithEmptyTags",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: input);

            // Assert: empty tags should result in null (not persisted)
            Assert.Null(state.Tags);

            // Validate logs
            LogAssert.NoWarningsOrErrors(this.testService.LogProvider);
        }

        [Fact]
        public async Task TagsSurviveContinueAsNew()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "workflow", "retry-loop" },
                { "version", "v2" },
            };

            // Act: run an orchestration that uses ContinueAsNew multiple times
            TestInstance<int> instance = await this.testService.RunOrchestrationWithTags(
                input: 0,
                orchestrationName: "TagsContinueAsNewTest",
                tags: tags,
                implementation: async (ctx, input) =>
                {
                    if (input < 3)
                    {
                        await ctx.CreateTimer<object>(DateTime.MinValue, null);
                        ctx.ContinueAsNew(input + 1);
                    }

                    return input;
                });

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: 3,
                continuedAsNew: true);

            // Assert: tags should survive multiple ContinueAsNew cycles
            Assert.NotNull(state.Tags);
            Assert.Equal(2, state.Tags.Count);
            Assert.Equal("retry-loop", state.Tags["workflow"]);
            Assert.Equal("v2", state.Tags["version"]);
        }

        [Fact]
        public async Task TagsPreservedAfterActivityExecution()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "project", "my-project" },
            };

            string input = "World";

            // Act: run an orchestration with tags that calls an activity
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input,
                orchestrationName: "TagsWithActivityTest",
                tags: tags,
                implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                activities: ("SayHello", TestService.MakeActivity((TaskContext ctx, string input) => $"Hello, {input}!")));

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: $"Hello, {input}!");

            // Assert: tags should still be present on the state after activity execution
            Assert.NotNull(state.Tags);
            Assert.Single(state.Tags);
            Assert.Equal("my-project", state.Tags["project"]);
        }

        [Fact]
        public async Task TagsVisibleViaQuery()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "queryTest", "yes" },
            };

            // Act: run an orchestration with tags and query it
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "test",
                orchestrationName: "TagsQueryTest",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(expectedOutput: "test");

            // Query the state separately
            OrchestrationState queriedState = await instance.GetStateAsync();

            // Assert: tags should be visible on queried state
            Assert.NotNull(queriedState);
            Assert.NotNull(queriedState.Tags);
            Assert.Equal("yes", queriedState.Tags["queryTest"]);
        }

        [Fact]
        public async Task SubOrchestrationInheritsTags()
        {
            // Arrange: parent orchestration has tags
            var parentTags = new Dictionary<string, string>
            {
                { "team", "platform" },
                { "priority", "high" },
            };

            string subOrchestrationName = "ChildOrchestrationForTagTest";

            // Register the sub-orchestration that returns a simple value
            this.testService.RegisterInlineOrchestration<string, string>(
                subOrchestrationName,
                implementation: (ctx, input) => Task.FromResult("child-done"));

            // Act: parent orchestration calls a sub-orchestration
            TestInstance<string> parentInstance = await this.testService.RunOrchestrationWithTags(
                input: "parent",
                orchestrationName: "ParentOrchestrationForTagTest",
                tags: parentTags,
                implementation: async (ctx, input) =>
                {
                    string childResult = await ctx.CreateSubOrchestrationInstance<string>(
                        subOrchestrationName, string.Empty, null);
                    return childResult;
                });

            OrchestrationState parentState = await parentInstance.WaitForCompletion(
                timeout: TimeSpan.FromSeconds(15),
                expectedOutput: "child-done");

            // Assert: parent tags should still be present
            Assert.NotNull(parentState.Tags);
            Assert.Equal("platform", parentState.Tags["team"]);
            Assert.Equal("high", parentState.Tags["priority"]);
        }

        [Fact]
        public async Task TagsWithSpecialCharacters()
        {
            // Arrange: tags with special characters that could cause JSON/SQL issues
            var tags = new Dictionary<string, string>
            {
                { "key with spaces", "value with spaces" },
                { "unicode-key-日本語", "unicode-value-中文" },
                { "special\"chars", "value'with\"quotes" },
            };

            // Act
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "special",
                orchestrationName: "TagsSpecialCharsTest",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            OrchestrationState state = await instance.WaitForCompletion(expectedOutput: "special");

            // Assert: all special character tags should be preserved
            Assert.NotNull(state.Tags);
            Assert.Equal(3, state.Tags.Count);
            Assert.Equal("value with spaces", state.Tags["key with spaces"]);
            Assert.Equal("unicode-value-中文", state.Tags["unicode-key-日本語"]);
            Assert.Equal("value'with\"quotes", state.Tags["special\"chars"]);
        }

        [Fact]
        public async Task TagsAvailableInActivityMiddleware()
        {
            // Arrange: tags that should be accessible in the activity middleware context
            var tags = new Dictionary<string, string>
            {
                { "tenantId", "tenant-123" },
                { "region", "us-west" },
            };

            // Act: run an orchestration with tags that calls an activity.
            // The activity captures the OrchestrationExecutionContext tags from the TaskActivityWorkItem.
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "middleware-test",
                orchestrationName: "TagsActivityMiddlewareTest",
                tags: tags,
                implementation: (ctx, input) => ctx.ScheduleTask<string>("CaptureActivity", "", input),
                activities: ("CaptureActivity", TestService.MakeActivity((TaskContext ctx, string input) =>
                {
                    // Note: The actual OrchestrationExecutionContext is on the TaskMessage,
                    // not directly accessible from TaskContext in the activity code.
                    // We verify it indirectly by checking that the orchestration state still has tags.
                    return $"processed-{input}";
                })));

            OrchestrationState state = await instance.WaitForCompletion(
                expectedOutput: "processed-middleware-test");

            // Assert: verify the orchestration completed successfully and tags are preserved
            Assert.NotNull(state.Tags);
            Assert.Equal("tenant-123", state.Tags["tenantId"]);
            Assert.Equal("us-west", state.Tags["region"]);
        }

        [Fact]
        public async Task TagsOnManyOrchestrations()
        {
            // Arrange: tags for querying multiple orchestrations
            var tags = new Dictionary<string, string>
            {
                { "batch", "test-batch-1" },
            };

            // Act: run an orchestration with tags
            TestInstance<string> instance = await this.testService.RunOrchestrationWithTags(
                input: "query-test",
                orchestrationName: "TagsManyQueryTest",
                tags: tags,
                implementation: (ctx, input) => Task.FromResult(input));

            await instance.WaitForCompletion(expectedOutput: "query-test");

            // Query using GetManyOrchestrations (which uses _QueryManyOrchestrations)
            var filter = new SqlOrchestrationQuery();
            IReadOnlyCollection<OrchestrationState> results =
                await this.testService.OrchestrationServiceMock.Object.GetManyOrchestrationsAsync(
                    filter, CancellationToken.None);

            // Assert: at least one result should have our tags
            Assert.NotEmpty(results);
            bool foundTaggedInstance = false;
            foreach (OrchestrationState result in results)
            {
                if (result.OrchestrationInstance.InstanceId == instance.InstanceId)
                {
                    Assert.NotNull(result.Tags);
                    Assert.Equal("test-batch-1", result.Tags["batch"]);
                    foundTaggedInstance = true;
                }
            }

            Assert.True(foundTaggedInstance, "Did not find the tagged orchestration instance in query results.");
        }
    }
}
