// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.AzureFunctions
{
    using System;
    using Newtonsoft.Json;

    class ManagedIdentityOptions
    {
        [JsonProperty("authorityHost")]
        public Uri? AuthorityHost { get; set; }

        [JsonProperty("tenantId")]
        public string? TenantId { get; set; }

        [JsonProperty("useAzureManagedIdentity")]
        public bool UseAzureManagedIdentity { get; set; }
    }
}