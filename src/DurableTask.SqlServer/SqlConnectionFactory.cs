// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Threading.Tasks;

using Azure.Core;
using Azure.Identity;

using Microsoft.Data.SqlClient;

namespace DurableTask.SqlServer
{
    internal class SqlConnectionFactory
    {
        readonly string connectionString;
        readonly ManagedIdentitySettings? managedIdentitySettings;

        public SqlConnectionFactory(string connectionString, ManagedIdentitySettings? managedIdentitySettings = null)
        {
            this.connectionString = connectionString;
            this.managedIdentitySettings = managedIdentitySettings;
        }

        public async Task<SqlConnection> CreateConnection()
        {
            var connection = new SqlConnection(this.connectionString);
            if (this.managedIdentitySettings != null && this.managedIdentitySettings.UseAzureManagedIdentity)
            {
                var azureCredentialOptions = new DefaultAzureCredentialOptions();
                if (this.managedIdentitySettings.AuthorityHost != null)
                {
                    azureCredentialOptions.AuthorityHost = this.managedIdentitySettings.AuthorityHost;
                }
                if (!string.IsNullOrEmpty(this.managedIdentitySettings.TenantId))
                {
                    azureCredentialOptions.InteractiveBrowserTenantId = this.managedIdentitySettings.TenantId;
                }
                var azureCredential = new DefaultAzureCredential(azureCredentialOptions);
                var requestContext = new TokenRequestContext(new string[] { ManagedIdentitySettings.Resource });
                var accessToken = await azureCredential.GetTokenAsync(requestContext);
                connection.AccessToken = accessToken.Token;
            }

            return connection;
        }
    }
}