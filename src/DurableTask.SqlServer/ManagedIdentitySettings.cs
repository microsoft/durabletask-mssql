// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
using System;
using Newtonsoft.Json;

namespace DurableTask.SqlServer
{
    /// <summary>
    /// Configuration options for Azure Managed Identity.
    /// </summary>
    public class ManagedIdentitySettings
    {
        const string DefaultResource = "https://database.windows.net/.default";

        /// <summary>
        /// Initializes a new instance of the <see cref="ManagedIdentitySettings"/> class.
        /// </summary>
        /// <param name="resource">The resource of the Azure SQL endpoint.</param>
        /// <param name="authorityHost">The host of the Azure Active Directory authority.</param>
        /// <param name="tenantId">The tenant id of the user to authenticate.</param>
        public ManagedIdentitySettings(Uri? resource = null, Uri? authorityHost = null, string? tenantId = null)
        {
            this.Resource = resource ?? new Uri(DefaultResource);
            this.AuthorityHost = authorityHost;
            this.TenantId = tenantId;
        }

        /// <summary>
        /// The Azure SQL resource with which to authenticate. The default is https://database.windows.net/.default.
        /// </summary>
        /// <remarks>
        /// See https://docs.microsoft.com/azure/active-directory/managed-identities-azure-resources/services-support-managed-identities#azure-sql for the list of valid resource URIs.
        /// </remarks>
        [JsonProperty("resource")]
        public Uri Resource { get; set; }

        /// <summary>
        /// The host of the Azure Active Directory authority. The default is https://login.microsoftonline.com/.
        /// </summary>
        /// <remarks>
        /// See https://docs.microsoft.com/azure/active-directory/develop/authentication-national-cloud#azure-ad-authentication-endpoints for the list of valid Azure AD authority hosts.
        /// </remarks>
        [JsonProperty("authorityHost")]
        public Uri? AuthorityHost { get; set; }

        /// <summary>
        /// The tenant id of the user to authenticate.
        /// </summary>
        [JsonProperty("tenantId")]
        public string? TenantId { get; set; }

        /// <summary>
        /// Use Azure Managed Identity to connect to SQL Server
        /// </summary>
        [JsonProperty("useAzureManagedIdentity")]
        public bool UseAzureManagedIdentity { get; set; }
    }
}