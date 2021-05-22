// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using Newtonsoft.Json;

namespace DurableTask.SqlServer
{
    /// <summary>
    /// Configuration options for Managed Identity.
    /// </summary>
    public class ManagedIdentitySettings
    {
        public const string Resource = "https://database.windows.net/";

        /// <summary>
        /// Initializes a new instance of the <see cref="ManagedIdentitySettings"/> class.
        /// </summary>
        /// <param name="authorityHost">The host of the Azure Active Directory authority.</param>
        /// <param name="tenantId">The tenant id of the user to authenticate.</param>
        public ManagedIdentitySettings(Uri? authorityHost = null, string? tenantId = null)
        {
            this.AuthorityHost = authorityHost;
            this.TenantId = tenantId;
        }

        /// <summary>
        /// The host of the Azure Active Directory authority. The default is https://login.microsoftonline.com/.
        /// </summary>
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