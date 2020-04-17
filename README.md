# Introduction

DurableTask.SqlServer (final name TBD) is a backend storage provider for the Durable Task Framework (DTFx).
It persists all task hub state in a SQL Server database, which can be hosted in Azure or in your own infrastructure.
This provider includes support for all DTFx features, including orchestrations, activities, and entities.

## When to choose DurableTask.SqlServer

Each DTFx storage provider has unique strengths and weaknesses. The specific provider you choose will depend
on many factors that are specific to your technical domain. We recommend you choose DurableTask.SqlServer if
the following factors are important to you:

1. **Strong consistency**: All data stored in the task hub is transactionally consistent, minimizing the chance of
duplicate execution that is more common in eventually-consistent storage providers.
2. **Portability**: SQL Server is available as a fully managed [Azure SQL Database](https://azure.microsoft.com/services/sql-database/)
service as well as a standalone installation on your own Windows or Linux servers or containers. Other cloud providers also support
hosted SQL Server installations, including [Amazon RDS for SQL Server](https://aws.amazon.com/rds/sqlserver/) and
[Google's Cloud SQL for SQL Server](https://cloud.google.com/sql/docs/sqlserver).
3. **Extensibility**: You have full access to the database schema and have the ability to create custom views,
tune indexes, and create triggers.
4. **Predictable and scalable pricing**: SQL Server and Azure SQL database offer a variety of scalable pricing options,
including free, [serverless](https://docs.microsoft.com/azure/sql-database/sql-database-serverless), per-core, and
volume licensing. Managed offerings also allow you to scale up or down your database with minimal effort and zero downtime.
5. **Enterprise capabilities**: SQL Server is a highly scalable relational database designed for mission-critical workloads.
It includes many features around disaster recovery, replication, and security that are not available in other storage providers.

DurableTask.SqlServer may not be appropriate if you workload has extreme throughput requirements that aren't suitable
for relational databases like SQL Server.

## Getting started

Install the following tools to get started:

* [.NET Core 3.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1)
* [SQL Server Management Studio](https://docs.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms) (Windows only - future updates will remove this requirement)

The free developer edition of SQL Server can be downloaded [here](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).
You will also need an instance of a SQL database named `TaskHub` (future updates will allow customizeable database names).
The recommended database collation is `Latin1_General_100_BIN2_UTF8`.

The database itself must be manually created. Then, using SQL Server Management Studio, open it and execute the contents of
the `src/DurableTask.SqlServer/Scripts/create-schema.sql` script file. At this point, you should be able to build and run
the automated tests against a local SQL Server instance.