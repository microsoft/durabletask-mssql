// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
