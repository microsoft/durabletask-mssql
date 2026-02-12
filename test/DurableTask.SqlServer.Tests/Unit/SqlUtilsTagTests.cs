// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlTypes;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using Xunit;

    public class SqlUtilsTagTests
    {
        [Fact]
        public void GetTags_ExecutionStartedEvent_WithTags_ReturnsSerialized()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
            };

            var startedEvent = new ExecutionStartedEvent(-1, "input")
            {
                Tags = tags,
            };

            // Act
            SqlString result = SqlUtils.GetTags(startedEvent);

            // Assert
            Assert.False(result.IsNull);
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(result.Value);
            Assert.Equal(2, deserialized.Count);
            Assert.Equal("value1", deserialized["key1"]);
            Assert.Equal("value2", deserialized["key2"]);
        }

        [Fact]
        public void GetTags_ExecutionStartedEvent_NullTags_ReturnsNull()
        {
            // Arrange
            var startedEvent = new ExecutionStartedEvent(-1, "input")
            {
                Tags = null,
            };

            // Act
            SqlString result = SqlUtils.GetTags(startedEvent);

            // Assert
            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTags_ExecutionStartedEvent_EmptyTags_ReturnsNull()
        {
            // Arrange
            var startedEvent = new ExecutionStartedEvent(-1, "input")
            {
                Tags = new Dictionary<string, string>(),
            };

            // Act
            SqlString result = SqlUtils.GetTags(startedEvent);

            // Assert
            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTags_NonStartedEvent_ReturnsNull()
        {
            // Arrange - use a non-ExecutionStartedEvent
            var completedEvent = new ExecutionCompletedEvent(
                -1, "result", OrchestrationStatus.Completed);

            // Act
            SqlString result = SqlUtils.GetTags(completedEvent);

            // Assert
            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTags_SpecialCharacters_RoundTrips()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "special\"key", "value'with\"quotes" },
                { "unicode-日本語", "中文" },
                { "key with spaces", "value with spaces" },
            };

            var startedEvent = new ExecutionStartedEvent(-1, "input")
            {
                Tags = tags,
            };

            // Act
            SqlString result = SqlUtils.GetTags(startedEvent);

            // Assert
            Assert.False(result.IsNull);
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(result.Value);
            Assert.Equal(3, deserialized.Count);
            Assert.Equal("value'with\"quotes", deserialized["special\"key"]);
            Assert.Equal("中文", deserialized["unicode-日本語"]);
            Assert.Equal("value with spaces", deserialized["key with spaces"]);
        }

        [Fact]
        public void GetTagsFromContext_WithTags_ReturnsSerialized()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "tenantId", "tenant-123" },
            };

            string contextJson = JsonConvert.SerializeObject(new { OrchestrationTags = tags });
            var context = JsonConvert.DeserializeObject<OrchestrationExecutionContext>(contextJson);

            var message = new TaskMessage
            {
                Event = new TaskScheduledEvent(-1),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "test-instance",
                    ExecutionId = "test-execution",
                },
                OrchestrationExecutionContext = context,
            };

            // Act
            SqlString result = SqlUtils.GetTagsFromContext(message);

            // Assert
            Assert.False(result.IsNull);
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(result.Value);
            Assert.Single(deserialized);
            Assert.Equal("tenant-123", deserialized["tenantId"]);
        }

        [Fact]
        public void GetTagsFromContext_NullContext_ReturnsNull()
        {
            // Arrange
            var message = new TaskMessage
            {
                Event = new TaskScheduledEvent(-1),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "test-instance",
                    ExecutionId = "test-execution",
                },
                OrchestrationExecutionContext = null,
            };

            // Act
            SqlString result = SqlUtils.GetTagsFromContext(message);

            // Assert
            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTagsFromContext_EmptyTags_ReturnsNull()
        {
            // Arrange
            string contextJson = JsonConvert.SerializeObject(new { OrchestrationTags = new Dictionary<string, string>() });
            var context = JsonConvert.DeserializeObject<OrchestrationExecutionContext>(contextJson);

            var message = new TaskMessage
            {
                Event = new TaskScheduledEvent(-1),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "test-instance",
                    ExecutionId = "test-execution",
                },
                OrchestrationExecutionContext = context,
            };

            // Act
            SqlString result = SqlUtils.GetTagsFromContext(message);

            // Assert
            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTagsFromContext_ContextWithNullTags_ReturnsNull()
        {
            // Arrange - default OrchestrationExecutionContext has null tags
            var context = new OrchestrationExecutionContext();

            var message = new TaskMessage
            {
                Event = new TaskScheduledEvent(-1),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "test-instance",
                    ExecutionId = "test-execution",
                },
                OrchestrationExecutionContext = context,
            };

            // Act
            SqlString result = SqlUtils.GetTagsFromContext(message);

            // Assert
            Assert.True(result.IsNull);
        }
    }
}
