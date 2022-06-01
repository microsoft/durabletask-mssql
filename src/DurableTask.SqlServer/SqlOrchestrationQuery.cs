// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;

    /// <summary>
    /// Represents database orchestration query parameters.
    /// </summary>
    public class SqlOrchestrationQuery
    {
        /// <summary>
        /// The maximum number of records to return.
        /// </summary>
        public int PageSize { get; set; } = 100;

        /// <summary>
        /// Gets or sets the zero-indexed page number for paginated queries.
        /// </summary>
        public int PageNumber { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to fetch orchestration inputs.
        /// </summary>
        public bool FetchInput { get; set; } = true;

        /// <summary>
        /// Gets or sets a value indicating whether to fetch orchestration outputs.
        /// </summary>
        public bool FetchOutput { get; set; } = true;

        /// <summary>
        /// Gets or sets a minimum creation time filter. Only orchestrations created
        /// after this date are selected.
        /// </summary>
        public DateTime CreatedTimeFrom { get; set; }
        
        /// <summary>
        /// Gets or sets a maximum creation time filter. Only orchestrations created
        /// before this date are selected.
        /// </summary>
        public DateTime CreatedTimeTo { get; set; }

        /// <summary>
        /// Gets or sets a set of orchestration status values to filter orchestrations by.
        /// </summary>
        public ISet<OrchestrationStatus>? StatusFilter { get; set; }

        /// <summary>
        /// Gets or sets an instance ID prefix to use for filtering orchestration instances.
        /// </summary>
        public string? InstanceIdPrefix { get; set; }

        /// <summary>
        /// Determines whether the query will retrieve only parent instances.
        /// </summary>
        public bool FetchParentInstancesOnly { get; set; } = false;
    }
}
