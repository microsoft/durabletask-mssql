# Introduction

The Durable Task SQL Provider is a backend for the [Durable Task Framework](https://github.com/Azure/durabletask) (DTFx) and [Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview) that persists all task hub state in a Microsoft SQL database. It's compatible with [on-premises SQL Server](https://www.microsoft.com/sql-server/), [SQL Server for Docker containers](https://hub.docker.com/_/microsoft-mssql-server), the cloud-hosted [Azure SQL Database](https://azure.microsoft.com/services/azure-sql/), and includes support for orchestrations, activities, and durable entities.

## Features

The Microsoft SQL provider is just one of [many supported providers for the Durable Task Framework](https://github.com/Azure/durabletask#supported-persistance-stores). Each backend storage provider has its own strengths and weaknesses. We believe that the Microsoft SQL provider has many strengths that make it worth creating and supporting.

### Portability

Microsoft SQL Server is an industry leading database server available as a managed service or as a standalone installation and is supported by the leading cloud providers ([Azure SQL](https://azure.microsoft.com/services/azure-sql/), [SQL Server on AWS](https://aws.amazon.com/sql/), [Google Cloud SQL](https://cloud.google.com/sql/), etc.). It also is supported on multiple OS platforms, like [Windows Server](https://www.microsoft.com/sql-server/), [Linux Docker containers](https://hub.docker.com/_/microsoft-mssql-server), and more recently on [IoT/Edge](https://azure.microsoft.com/services/sql-edge/) devices. All your orchestration data is contained in a single database that can easily be exported from one host to another, so there is no need to worry about having your data locked to a particular vendor.

### Control

The DTFx schemas can be provisioned into your own SQL database, allowing you to secure it any way you want and incorporate it into existing business continuity processes ([backup/restore](https://docs.microsoft.com/azure/azure-sql/database/automated-backups-overview), [disaster recovery](https://docs.microsoft.com/azure/azure-sql/database/auto-failover-group-overview), etc.). This also means you can easily integrate Durable Functions or DTFx with existing line-of-business applications by co-hosting the data in the same database and leveraging built-in stored procedures to have apps directly interact with orchestrations or entities from SQL. Having control of your database also means that you can scale up or down the corresponding server as needed to meet your price-performance needs - or just let the Azure platform do this automatically with their hosted [Serverless tier](https://docs.microsoft.com/azure/azure-sql/database/serverless-tier-overview).

### Simplicity

This provider was designed from the ground-up with simplicity in mind. The data is transactionally consistent and it's easy to query the tables and views directly using existing tools like the cross-platform [mssql-cli](https://docs.microsoft.com/sql/tools/mssql-cli), [SQL Server Management Studio](https://docs.microsoft.com/sql/ssms), or the [Azure Portal](https://docs.microsoft.com/azure/azure-sql/database/connect-query-portal) if your SQL database is hosted in Azure. Unlike the default Azure Storage provider, the SQL provider backend does not require you to configure partition counts. A single database can support any number of nodes running orchestrations and entities, provided it has sufficient CPU power to handle the required load. You also don't pay any unexpected performance penalties for larger function inputs and outputs.

### Multitenancy

One of the goals for this provider is to create a foundation for safe [multi-tenant deployments](https://en.wikipedia.org/wiki/Multitenancy). This is especially valuable when your organization has many small apps but prefers to manage only a single backend database. Different apps can connect to this database using different database login credentials. Database administrators will be able to query data across all tenants but individual apps will only have access to their own data. When further isolation and security is needed, each app can [have its own schema](multitenancy.md#managing-custom-schemas). Note that tenant-specific code that runs _outside_ the database would still be expected to run on appropriately isolated compute instances. Learn how to get started with multitenancy [here](multitenancy.md).

## FAQ

**Q. Does this require Azure?**

No. You can run on Azure if you want, but this provider was designed specifically to support running DTFx and Durable Functions in a non-Azure environment. In fact, it's the first production-grade provider that supports non-Azure deployments.

**Q. When would I choose this over the Azure Storage provider?**

* If you want to build cloud-agnostic apps - Microsoft SQL databases can be run [anywhere](#portability)
* If you need predictable and performance-scalable pricing
* If you need to scale past the Azure Storage provider's limit of 16 partitions
* If you need enterprise features like data encryption or business continuity features
* If you want direct access to the data - which is supported via SQL views and stored procedures
* If you want multitenancy within a single database

**Q. Why Microsoft SQL (and not an OSS database, like PostgreSQL)?**

* Extremely efficient in dealing with large data payloads
* Flexible cloud hosting, including a unique [Azure SQL Serverless tier](https://docs.microsoft.com/azure/azure-sql/)
* Already in use by many organizations, with a proven track record
* Opinionated database support means we can take advantage of native database features

## Contact and support

* Create a [GitHub issue](https://github.com/microsoft/durabletask-mssql/issues) for bug reports, feature requests, or questions.
* Follow [@cgillum](https://twitter.com/cgillum) and [@AzureFunctions](https://twitter.com/AzureFunctions) on Twitter for announcements.
* Add a ⭐️ star on GitHub or ❤️ tweets to support the project!

## License

This project is licensed under the [MIT license](https://github.com/microsoft/durabletask-mssql/blob/main/LICENSE).

Copyright (c) Microsoft Corporation.
