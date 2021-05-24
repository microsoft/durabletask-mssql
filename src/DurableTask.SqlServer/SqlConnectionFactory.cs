// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
using System.Threading.Tasks;

using Azure.Core;
using Azure.Identity;

using Microsoft.Data.SqlClient;

    class SqlConnectionFactory
{
    internal class SqlConnectionFactory
    {
        readonly string connectionString;
        readonly ManagedIdentitySettings? managedIdentitySettings;

        AccessToken cachedAccessToken;

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
                AccessToken accessToken = await this.GetAccessToken(this.managedIdentitySettings);
                connection.AccessToken = accessToken.Token;
            }

            return connection;
        }

        async ValueTask<AccessToken> GetAccessToken(ManagedIdentitySettings managedIdentitySettings)
        {
            if (managedIdentitySettings == null)
            {
                throw new ArgumentNullException(nameof(managedIdentitySettings));
            }

            // Try to use the cached access token to avoid the overhead of interacting with
            // Azure.Identity client libraries, which can be expensive on some platforms.
            // Subtract 5 minutes from expiration time to account for potential clock skew.
            if (this.cachedAccessToken.Token != null &&
                this.cachedAccessToken.ExpiresOn > DateTime.UtcNow.AddMinutes(-5))
            {
                return this.cachedAccessToken;
            }

                var azureCredentialOptions = new DefaultAzureCredentialOptions();
            if (managedIdentitySettings.AuthorityHost != null)
                {
                azureCredentialOptions.AuthorityHost = managedIdentitySettings.AuthorityHost;
                }

            if (!string.IsNullOrEmpty(managedIdentitySettings.TenantId))
                {
                azureCredentialOptions.InteractiveBrowserTenantId = managedIdentitySettings.TenantId;
            }

            var azureCredential = new DefaultAzureCredential(azureCredentialOptions);
            var requestContext = new TokenRequestContext(new string[] { managedIdentitySettings.Resource.AbsoluteUri });
            this.cachedAccessToken = await azureCredential.GetTokenAsync(requestContext);
            return this.cachedAccessToken;
        }
    }
}