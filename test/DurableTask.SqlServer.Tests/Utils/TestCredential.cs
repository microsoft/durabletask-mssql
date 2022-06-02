// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Utils
{
    public class TestCredential
    {
        public TestCredential(string userId, string connectionString)
        {
            this.UserId = userId;
            this.ConnectionString = connectionString;
        }

        public string UserId { get; }

        public string ConnectionString { get; }
    }
}
