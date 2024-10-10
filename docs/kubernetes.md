# Getting started with Kubernetes

This guide goes through the steps required for configuring Durable Functions or DTFx with the Microsoft SQL backend on a Kubernetes cluster. This guide assumes you already have a Kubernetes cluster available. If not, you can create one on AKS using [this guide](https://docs.microsoft.com/azure/aks/kubernetes-walkthrough-portal).

## Deploy the SQL database

These instructions assume you want to deploy a new database as part of your Kubernetes cluster. You can alternatively deploy a database in one of the managed clouds, like [Azure SQL Database](quickstart.md#azure-sql-database).

The following is an example YAML for deploying a MSSQL database into a Kubernetes cluster.

```yml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mssql-deployment
  labels:
    app: mssql
spec:
  replicas: 1
  selector:
     matchLabels:
       app: mssql
  template:
    metadata:
      labels:
        app: mssql
    spec:
      terminationGracePeriodSeconds: 30
      hostname: mssqlinst
      containers:
      - name: mssql
        image: mcr.microsoft.com/mssql/server:2019-latest
        ports:
        - containerPort: 1433
        env:
        - name: MSSQL_PID
          value: "Developer"
        - name: ACCEPT_EULA
          value: "Y"
        - name: SA_PASSWORD
          value: "PLACEHOLDER" # replace PLACEHOLDER With a password
---
apiVersion: v1
kind: Service
metadata:
  name: mssqlinst
spec:
  type: LoadBalancer # ClusterIP may be more appropriate in production
  selector:
    app: mssql
  ports:
    - protocol: TCP
      port: 1433
      targetPort: 1433
```

> A few caveats about the above deployment yaml sample:
>
> - The spec does not include any persistent file system volume configuration. If the container is restarted, all data in the server will be lost. You'll want to configure a persistent storage volume if you need your data to persist across container restarts.
> - The `SA_PASSWORD` environment variable value is in cleartext. Normally, passwords should be configured via Kubernetes secrets.
> - We've chosen a `type: LoadBalancer` for the Service. This allows external clients to access the SQL Server instance. However, you may wish to use `type: ClusterIP` instead if you do not want external clients to be able to access your instance.

The above yaml can be saved to a file named **mssql-deployment.yml**. We'll deploy the MSSQL container to the `mssql` namespace using the following _kubectl_ command:

```powershell
kubectl apply -f ./mssql-deployment.yml -n mssql
```

Once the container starts up, use the following PowerShell commands to create a database:

```powershell
# Get the name of the Pod running SQL Server
$mssqlPod = kubectl get pods -n mssql -o jsonpath='{.items[0].metadata.name}'

# Use sqlcmd.exe to create a database named "DurableDB".
# Replace 'PLACEHOLDER' with the password you used earlier
kubectl exec -n mssql $mssqlPod -- /opt/mssql-tools18/bin/sqlcmd -S . -U sa -P "PLACEHOLDER" -Q "CREATE DATABASE [DurableDB] COLLATE Latin1_General_100_BIN2_UTF8"
```

?> If you have an old version of the database already deployed, you may want to first delete that one using `DROP DATABASE [DurableDB]` SQL command. This should only be necessary when using alpha builds of the Durable Task SQL provider. Newer builds will take care of database schema upgrades automatically.

The database is now ready to go. The database schema will be deployed automatically in a later step.

## Install KEDA

KEDA is used to automatically scale the app deployment based on load. KEDA can be installed using the [Azure Functions Core Tools](https://docs.microsoft.com/azure/azure-functions/functions-run-local). The Durable Task SQL provider requires KEDA v2.2 or greater, which is when the [mssql](https://keda.sh/docs/scalers/mssql/) scaler was first introduced.

```powershell
# install KEDA on this AKS cluster
func kubernetes install --namespace keda
```

## Deploy an Azure Functions app

Next we will deploy the performance testing app to the cluster and configure it to connect to our SQL database. If you'd like to build and deploy the container image yourself, you will first need to clone the following GitHub repo (otherwise, skip the next two steps):

```powershell
git clone https://github.com/microsoft/durabletask-mssql
```

You can then run the following command to create a docker container for the image. The `$repo` value should be the name of a docker repository you own.

```powershell
$repo = "<your-repo-name>"
docker build -t $repo/mssql-durable-functions:latest -f ./test/PerformanceTests/Dockerfile .
docker push $repo/mssql-durable-functions:latest
```

The docker app assumes there is an environment variable named `SQLDB_Connection` that contains the connection string. We deploy the connection string as a secret using the following YAML:

```yml
apiVersion: v1
kind: Secret
metadata:
  name: mssql-secrets
type: Opaque
stringData:
  # Replace PLACEHOLDER with the password you chose earlier
  SQLDB_Connection: "Server=mssqlinst.mssql.svc.cluster.local;Database=DurableDB;User ID=sa;Password=PLACEHOLDER;Persist Security Info=False;TrustServerCertificate=True;Encrypt=True;"
```

Name the yaml file **mssql-secrets.yml** and deploy it to your cluster.

```powershell
kubectl apply -f ./mssql-secrets.yml
```

Next, deploy the app using the following command:

```powershell
$deploymentName = "durabletask-mssql-app"
func kubernetes deploy --name $deploymentName --image-name "$repo/mssql-durable-functions:latest" --secret-name "mssql-secrets" --max-replicas 5
```

If successful, you should see output like this:

```
deployment "durabletask-mssql-app-http" successfully rolled out
        StartManyEntities - [httpTrigger]
        Invoke url: http://52.137.109.117/api/startmanyentities

        StartManyMixedOrchestrations - [httpTrigger]
        Invoke url: http://52.137.109.117/api/startmanymixedorchestrations

        StartManySequences - [httpTrigger]
        Invoke url: http://52.137.109.117/api/startmanysequences

        Master key: Phblx6kQt2dwcVMJfE9GMhPhzZtJ3s13xUndgZ/DpUe9K9GL0Ctew==
```

## Test the function app

Next we'll create load on the deployed app using the **StartManySequences** HTTP API listed in the deployment output. Before that, it may be useful to monitor the number of scale-out container instances using the **kubectl**. There are a few different ways to do this, but one option is to monitor the Horizontal Pod Autoscaler (HPA) that KEDA generated to see exactly how many deployment replicas that are being generated in realtime.

```powershell
kubectl get hpa keda-hpa-durabletask-mssql-app --watch
```

Next, use an HTTP client tool to send the following HTTP POST request, which will start multiple "hello cities" orchestrations in parallel:

```http
POST http://52.137.109.117/api/startmanysequences?count=100
```

At this point you should be able to observe that the number of replicas gradually increases to handle the load, and then decreases back to zero once the orchestrations have completed.
