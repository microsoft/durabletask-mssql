<p align="center">
  <a href="https://microsoft.github.io/durabletask-mssql/">
    <img alt="Durable Task SQL Provider" src="./docs/media/logo.png">
  </a>
</p>

# Microsoft SQL Provider for the Durable Task Framework

[![Build status](https://github.com/microsoft/durabletask-mssql/workflows/Build%20and%20Test/badge.svg)](https://github.com/microsoft/durabletask-mssql/actions?workflow=Build+and+Test)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

The Microsoft SQL provider for the [Durable Task Framework](https://github.com/Azure/durabletask) (DTFx) persists all task hub state in a Microsoft SQL database, which can be hosted in the cloud or in your own infrastructure.
This provider includes support for all DTFx features, including orchestrations, activities, and entities, and has full support for [Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview).

> ⚠ **IMPORTANT**: This project is currently under active development, working towards an initial _alpha_ release. Not all features are fully implemented and some names my change. It is not yet appropriate for use in production.

## Downloads

The Durable SQL provider for Durable Functions and DTFx are available as NuGet packages.

| Package | Latest Version | Description |
| ------- | -------------- | ----------- |
| Microsoft.DurableTask.SqlServer.AzureFunctions | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.SqlServer.AzureFunctions.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer.AzureFunctions/) | Use this package if building serverless Function apps with Azure Durable Functions. |
| Microsoft.DurableTask.SqlServer | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.SqlServer.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer/) | Use this package if using DTFx to build .NET apps. |

## Documentation

Want to learn more? Detailed information about this provider and getting started instructions can be found [here](https://microsoft.github.io/durabletask-mssql/).

## Code of Conduct

 This project has adopted the [Microsoft Open Source Code of conduct](https://opensource.microsoft.com/codeofconduct/).
 For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
