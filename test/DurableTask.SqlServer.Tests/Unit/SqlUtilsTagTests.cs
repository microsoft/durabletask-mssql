// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using DurableTask.Core;
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
    }
}
