param(
    [string]$subscription,
    [string]$appName,
    [string]$planName,
    [string]$funcGroup,
    [string]$sqlGroup,
    [string]$functionSKU="EP2",
    [int]$instanceCount=4,
    [string]$sqlDbName,
    [string]$sqlDbServer,
    [string]$sqlComputeModel="Provisioned", # Provisioned, Serverless
    [int]$sqlMinCpus=2,
    [int]$sqlCPUs=2, # Gen5 options: 2, 4, 8, 16, 24, 32, 40, 64, 80
    [int]$count=5000
)

$ErrorActionPreference = "Stop"

# Installing the Azure CLI: https://docs.microsoft.com/cli/azure/install-azure-cli
az account set -s $subscription

# Update the SQL SKU
# Reference: https://docs.microsoft.com/cli/azure/sql/db?view=azure-cli-latest#az_sql_db_update
Write-Host "Setting $sqlDbServer/$sqlDbName to the $sqlComputeModel compute model with (or up to) $sqlCPUs vCPUs..."
az sql db update --compute-model $sqlComputeModel --min-capacity $sqlMinCpus --capacity $sqlCPUs --family Gen5 --resource-group $sqlGroup --name $sqlDbName --server $sqlDbServer | Out-Null

# Update the plan
Write-Host "Setting $planName plan to max burst of $instanceCount instances..."

# Update the app with a minimum instance count
# NOTE: The order of these commands needs to change depending on whether we're adding or removing instances.
#       If adding, update the plan first. If subtracting, update the app first.
az functionapp plan update --resource-group $funcGroup --name $planName --sku $functionSKU --min-instances $instanceCount --max-burst $instanceCount | Out-Null
az resource update --resource-group $funcGroup --name "$appName/config/web" --set properties.minimumElasticInstanceCount=$instanceCount --resource-type "Microsoft.Web/sites" | Out-Null

Write-Host "Hard-restarting the app to ensure any plan changes take effect"
az functionapp stop --name $appName --resource-group $funcGroup
Sleep 10
az functionapp start --name $appName --resource-group $funcGroup
Sleep 10

$Stoploop = $false
[int]$Retrycount = 0
 
do {
    try {
        # ping the site to make sure it's up and running
        Write-Host "Pinging the app to ensure it can start-up"
        Invoke-RestMethod -Method Post -Uri "https://$appName.azurewebsites.net/admin/host/ping"
        $Stoploop = $true
    }
    catch {
        if ($Retrycount -gt 10){
            Write-Host "The app is still down after 10 ping retries. Giving up."
            return
        }
        else {
            Write-Host "Ping failed, which means the app is down. Retrying in 60 seconds..."
            Start-Sleep -Seconds 60
            $Retrycount = $Retrycount + 1
        }
    }
}
While ($Stoploop -eq $false)

# get the master key
$masterKey = (az functionapp keys list --name $appName --resource-group $funcGroup --query "masterKey" --output tsv)

# clear any data to make sure all tests start with the same amount of data in the database
Write-Host "Purging database of old instances"
Invoke-RestMethod -Method Post -Uri "https://$appName.azurewebsites.net/api/PurgeOrchestrationData?code=$masterKey"

# The Invoke-RestMethod command seems to run asynchronously, so sleep to give it time to finish
Write-Host "Sleeping for 15 seconds in case the previous command finished before the purge completed"
Sleep 15

# run the test with a prefix (example: "EP1-max1-sql4-10000")
if ($sqlComputeModel -eq "Serverless") {
    $prefix = "$functionSKU-max$instanceCount-sqlServerless-$count-"
} else { 
    $prefix = "$functionSKU-max$instanceCount-sql$sqlCPUs-$count-"
}
Write-Host "Starting test with prefix '$prefix'..."
$url = "https://$appName.azurewebsites.net/api/StartManySequences?count=$count&prefix=$prefix&code=$masterKey"
Write-Host $url
Invoke-RestMethod -Method Post -Uri $url