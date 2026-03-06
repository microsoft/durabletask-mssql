// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlTypes;
    using DurableTask.Core.History;
    using Microsoft.Data.SqlClient;
    using Newtonsoft.Json;
    using Xunit;

    public class SqlUtilsTagTests
    {
        [Fact]
        public void AddTagsParameter_WithTags_SetsJsonValue()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" },
            };

            using var command = new SqlCommand();

            // Act
            command.Parameters.AddTagsParameter(tags);

            // Assert
            SqlParameter param = command.Parameters["@Tags"];
            Assert.NotNull(param);
            Assert.Equal(SqlDbType.VarChar, param.SqlDbType);

            string json = (string)param.Value;
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            Assert.Equal(2, deserialized.Count);
            Assert.Equal("value1", deserialized["key1"]);
            Assert.Equal("value2", deserialized["key2"]);
        }

        [Fact]
        public void AddTagsParameter_NullTags_SetsDBNull()
        {
            using var command = new SqlCommand();

            // Act
            command.Parameters.AddTagsParameter(null);

            // Assert
            Assert.Equal(DBNull.Value, command.Parameters["@Tags"].Value);
        }

        [Fact]
        public void AddTagsParameter_EmptyTags_SetsDBNull()
        {
            using var command = new SqlCommand();

            // Act
            command.Parameters.AddTagsParameter(new Dictionary<string, string>());

            // Assert
            Assert.Equal(DBNull.Value, command.Parameters["@Tags"].Value);
        }

        [Fact]
        public void AddTagsParameter_SpecialCharacters_RoundTrips()
        {
            // Arrange
            var tags = new Dictionary<string, string>
            {
                { "special\"key", "value'with\"quotes" },
                { "unicode-日本語", "中文" },
                { "key with spaces", "value with spaces" },
            };

            using var command = new SqlCommand();

            // Act
            command.Parameters.AddTagsParameter(tags);

            // Assert
            string json = (string)command.Parameters["@Tags"].Value;
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            Assert.Equal(3, deserialized.Count);
            Assert.Equal("value'with\"quotes", deserialized["special\"key"]);
            Assert.Equal("中文", deserialized["unicode-日本語"]);
            Assert.Equal("value with spaces", deserialized["key with spaces"]);
        }

        [Fact]
        public void AddTagsParameter_TagsExceedMaxSize_ThrowsArgumentException()
        {
            // Arrange: create tags whose JSON serialization exceeds 8000 chars
            var tags = new Dictionary<string, string>
            {
                { "key", new string('x', 8000) },
            };

            using var command = new SqlCommand();

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => command.Parameters.AddTagsParameter(tags));
            Assert.Contains("exceeds the maximum allowed size of 8000", ex.Message);
        }

        [Fact]
        public void GetTagsJson_TagsExceedMaxSize_ThrowsArgumentException()
        {
            // Arrange: simulate merged tags that exceed 8000 chars
            // This covers the sub-orchestration merge path where individually-valid
            // parent + child tags combine to exceed the limit
            var tags = new Dictionary<string, string>
            {
                { "key", new string('x', 8000) },
            };

            var startedEvent = new ExecutionStartedEvent(-1, null) { Tags = tags };

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => SqlUtils.GetTagsJson(startedEvent));
            Assert.Contains("exceeds the maximum allowed size of 8000", ex.Message);
        }

        [Fact]
        public void GetTagsJson_NonExecutionStartedEvent_ReturnsNull()
        {
            // Non-ExecutionStartedEvent should return SqlString.Null
            var timerEvent = new TimerFiredEvent(-1);

            SqlString result = SqlUtils.GetTagsJson(timerEvent);

            Assert.True(result.IsNull);
        }

        [Fact]
        public void GetTagsJson_ExecutionStartedWithTags_ReturnsJson()
        {
            var tags = new Dictionary<string, string>
            {
                { "env", "prod" },
                { "team", "backend" },
            };

            var startedEvent = new ExecutionStartedEvent(-1, null) { Tags = tags };

            SqlString result = SqlUtils.GetTagsJson(startedEvent);

            Assert.False(result.IsNull);
            var deserialized = JsonConvert.DeserializeObject<Dictionary<string, string>>(result.Value);
            Assert.Equal(2, deserialized.Count);
            Assert.Equal("prod", deserialized["env"]);
            Assert.Equal("backend", deserialized["team"]);
        }
    }
}
