// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using Microsoft.Data.SqlClient;

    public static class SharedTestHelpers
    {
        public static string GetDefaultConnectionString()
        {
            // The default for local development on a Windows OS
            string defaultConnectionString = "Server=localhost;Database=DurableDB;Trusted_Connection=True;";

            // The use of SA_PASSWORD is intended for use with the mssql docker container
            string saPassword = Environment.GetEnvironmentVariable("SA_PASSWORD");
            if (string.IsNullOrEmpty(saPassword))
            {
                return defaultConnectionString;
            }

            var builder = new SqlConnectionStringBuilder(defaultConnectionString)
            {
                IntegratedSecurity = false,
                UserID = "sa",
                Password = saPassword,
            };

            return builder.ToString();
        }
    }
}
