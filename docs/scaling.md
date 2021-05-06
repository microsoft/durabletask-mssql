# Scaling

The Microsoft SQL Provider for the Durable Task Framework (DTFx) and Durable Functions is designed to run in elastic compute environments where nodes can be added or removed on-demand without introducing downtime. This article describes how scaling works and various options for configuring auto-scale.

## Terminology

Throughout this article, we'll use the term _worker_ to refer to a single replica of the DTFx backend. If you are building an app using DTFx directly, then _worker_ refers to an instance of the `TaskHubWorker` class. If you are building an app on the Azure Functions hosted service, then a _worker_ refers to a single instance of a function app. In the context of Kubernetes, a _worker_ typically corresponds to a deployment replica.

## Load balancing

The Durable SQL provider distributes orchestration and activity executions evenly across all workers that are configured for a particular [task hub](taskhubs.md). Each worker independently polls the database for work and will take on as much work as allowed by its [concurrency configuration settings](#concurrency-configuration) using a [competing consumer](https://docs.microsoft.com/azure/architecture/patterns/competing-consumers) load distribution strategy.

![Scale-out](media/arch-diagram.png)

Each worker replica is identical and capable of running _any_ orchestrator or activity task that it can fetch from the database. Assigning specific orchestrations or activities to specific workers is not supported. There's no hard limit to the number of workers that can be added to a task hub. The maximum number of workers is limited only by the amount of concurrent load that the SQL database can handle. If any worker fails or becomes unavailable, work will be automatically redistributed across the existing set of active workers within a few minutes.

?> If you're familiar with the Azure Storage backend for DTFx and Durable Functions, one key difference with SQL provider is that orchestration executions can theoretically scale-out to any number of workers. There is no concept of partitions or leases.

## Concurrency configuration

Each task hub worker can execute multiple orchestration events and activity tasks concurrently. The actual number of events or tasks that execute concurrently is configurable and is one of the key factors that impacts scalability. For in-process .NET apps, you can specify concurrency settings in the `SqlOrchestrationServiceSettings` class. The following example code configures both the maximum number of concurrent activity tasks and orchestrator events to be the number of cores on the VM.

```csharp
var settings = new SqlOrchestrationServiceSettings
{
    MaxConcurrentActivities = Environment.ProcessorCount,
    MaxActiveOrchestrations = Environment.ProcessorCount,
};

var service = new SqlOrchestrationService(settings);
var worker = new TaskHubWorker(service);
```

When using Azure Functions, these values inferred from the existing `maxConcurrentOrchestratorFunctions` and `maxConcurrentActivityFunctions` settings in the [host.json file](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-bindings#host-json), as shown in the following example:

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "maxConcurrentOrchestratorFunctions": 8,
      "maxConcurrentActivityFunctions": 8,
      "storageProvider": {
        "type": "mssql",
        "connectionStringName": "SQLDB_Connection"
      }
    }
  }
}
```

The values you select will vary depending on your expected workload. For example, if your activities are CPU-intensive or consume lots of memory, then you'll likely want to configure a smaller value for activity concurrency. Similarly, if your orchestrations have large history payloads (because of large inputs, outputs, etc.) then you should consider smaller orchestration concurrency configuration values. Choosing this configuration carefully is important to ensure your app has the right balance of performance and reliability.

?> Future versions of the Durable SQL provider may support automatic concurrency configuration based on available CPU, memory, and other metrics. However, until this support is available, it is recommended that you use performance and scale testing to determine the right concurrency configuration values for your expected workload.

## Worker auto-scale

The Durable SQL provider makes worker scale-out and scale-in recommendations based on the number of active and pending orchestration and activity tasks at any given time. The recommended number of workers is determined by dividing the current task backlog by the configured maximum per-worker concurrency settings. The basic formula looks like the following pseudocode:

```pseudocode
live_activities = rowcount(dt.Activities)
live_orchestrators = rowcount(dt.Instances WHERE #events > 0)
recommended_activity_workers = ceil(live_activities / max_concurrent_activities)
recommended_orchestrator_workers = ceil(live_orchestrators / max_concurrent_orchestrators)
recommended_worker_count = recommended_activity_workers + recommended_orchestrator_workers
```

Here are the English definitions of the variables mentioned in this algorithm:

| Variable | Description |
|-|-|
| *live_activities* | The number of rows in the `dt.NewTasks` table. This represents both activity tasks being actively processed and those waiting to be processed. |
| *max_concurrent_activities* | The maximum number of activities that can run concurrently on a single worker. This number is [configurable](#concurrency-configuration). |
| *recommended_activity_workers* | The number of worker replicas needed to handle all active and pending activities (i.e. `live_activities`). |
| *live_orchestrators* | The number of orchestration instances that are either active in memory or have events pending in the `dt.NewEvents` table. This does not include timer events scheduled in the future. |
| *max_concurrent_orchestrators* | The maximum number of orchestrations that can run concurrently (i.e. active in memory, not idle) on a single worker. This number is [configurable](#concurrency-configuration). |
| *recommended_orchestrator_workers* | The number of worker replicas needed to handle all active and pending orchestrator events (i.e. `live_orchestrators`). Each orchestrator must run on a single worker at a time so the actual number of events per orchestrator does not matter. |
| *recommended_worker_count* | The total number of workers needed to handle all activity tasks and orchestrator events. |

This value can be calculated automatically using either the `dt.GetScaleRecommendation` SQL function, which takes concurrency settings as parameters, or the `SqlOrchestrationService.GetScaleRecommendation` .NET API, which discovers the concurrency settings from configuration. The final number can then be given to an auto-scale compute component to change the number of allocated worker replicas.

If you're using the Durable SQL provider with [Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable) running on the [Elastic Premium Plan](https://docs.microsoft.com/azure/azure-functions/functions-premium-plan), then auto-scaling the number of app instances is managed automatically if you enable runtime scale monitoring as described [here](https://docs.microsoft.com/azure/azure-functions/functions-networking-options#premium-plan-with-virtual-network-triggers). Note that this doesn't require you to configure any virtual networking features.

!> The Azure Functions Consumption plan does not yet support Durable Functions apps configured with the Durable SQL provider.

If you are running your app in Kubernetes and have [KEDA](https://keda.sh) installed in your cluster, you can use the [MSSQL](https://keda.sh/docs/scalers/mssql/) scaler to automatically scale your app deployment instances. The following is an example `ScaledObject` configuration that can be used.

```yml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: mssql-scaledobject
spec:
  scaleTargetRef:
    name: durabletask-mssql-app
  triggers:
  - type: mssql
    metadata:
      connectionStringFromEnv: SQLDB_Connection
      targetValue: "1"
      query: "SELECT dt.GetScaleRecommendation(8, 8)"
```

?> Note that the two parameters for the `dt.GetScaleRecommendation` SQL function are values for `@MaxOrchestrationsPerWorker` and `@MaxActivitiesPerWorker` respectively.

The `targetValue` should always be `"1"` when using the `dt.GetScaleRecommendation` SQL function in the `query` property. This ensures there is a 1:1 mapping between workers and deployment replicas.

!> Make sure that the database credentials used by the `ScaledObject` are the same as those used by the app. Otherwise the `dt.GetScaledRecommendation` might return incorrect recommendations. See the [Multitenancy](multitenancy.md) topic for more information about how database credentials are mapped to task hubs.

## SQL database scale-out

The current version of the Durable SQL provider supports connecting to a single database instance. In many cases, the database will be the primary performance bottleneck. The recommended way to scale-out the database compute capacity is to increase the number of cores allocated to the SQL Server instance. Instructions for scaling up a SQL Server instance is out of scope for this article. However, if you are using [Azure SQL Database](https://docs.microsoft.com/azure/azure-sql/database/sql-database-paas-overview), you have the option of using the [Serverless tier](https://docs.microsoft.com/azure/azure-sql/database/serverless-tier-overview), which auto-scales the database based on CPU usage.
