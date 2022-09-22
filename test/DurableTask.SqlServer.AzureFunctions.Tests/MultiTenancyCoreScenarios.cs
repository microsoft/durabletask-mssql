// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.AzureFunctions.Tests
{
    using Xunit.Abstractions;

    public class MultiTenancyCoreScenarios : CoreScenarios
    {
        public MultiTenancyCoreScenarios(ITestOutputHelper output)
            : base(output, "TaskHubWithMultiTenancy", true)
        {
        }
    }
}