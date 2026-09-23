param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,

    [string]$DeploymentName = "main",

    [Parameter(Mandatory = $true)]
    [ValidateSet("Consume", "Publish")]
    [string]$Mode,

    [string]$Events,

    [double]$IntervalSeconds = 0.2
)

$ErrorActionPreference = "Stop"

$outputJson = az deployment group show `
    --resource-group $ResourceGroup `
    --name $DeploymentName `
    --query properties.outputs `
    --output json
if ($LASTEXITCODE -ne 0) { throw "Cannot read deployment outputs." }
$outputs = $outputJson | ConvertFrom-Json
foreach ($name in @("cosmosEndpoint", "cosmosDatabase", "cosmosContainer",
    "eventHubNamespace", "eventHubName", "consumerGroupName",
    "checkpointBlobUrl", "checkpointContainerName", "quarantineContainerName")) {
    if (-not $outputs.$name.value) {
        throw "Missing output '$name'. Deploy the revised template before using the Azure consumer."
    }
}

$env:COSMOS_ENDPOINT = $outputs.cosmosEndpoint.value
$env:COSMOS_DATABASE = $outputs.cosmosDatabase.value
$env:COSMOS_CONTAINER = $outputs.cosmosContainer.value
$env:COSMOS_QUARANTINE_CONTAINER = $outputs.quarantineContainerName.value
$env:EVENTHUB_NAME = $outputs.eventHubName.value
$env:EVENTHUB_FULLY_QUALIFIED_NAMESPACE = "$($outputs.eventHubNamespace.value).servicebus.windows.net"
$env:EVENTHUB_CONSUMER_GROUP = $outputs.consumerGroupName.value
$env:CHECKPOINT_BLOB_URL = $outputs.checkpointBlobUrl.value
$env:CHECKPOINT_CONTAINER = $outputs.checkpointContainerName.value

if ($Mode -eq "Publish" -and (-not $Events -or -not (Test-Path -LiteralPath $Events))) {
    throw "Publish requires -Events pointing to sync_demo's events.jsonl."
}
if ($Mode -eq "Publish") { $Events = (Resolve-Path -LiteralPath $Events).Path }

Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    if ($Mode -eq "Consume") {
        Write-Host "Starting consumer. Requires authorized network, Entra data roles and private DNS."
        python -m vsam_offload.eventhub_consumer
    }
    else {
        python -m vsam_offload.eventhub_publisher --events $Events --interval-seconds $IntervalSeconds
    }
    if ($LASTEXITCODE -ne 0) { throw "Azure $Mode failed (exit $LASTEXITCODE)." }
}
finally {
    Pop-Location
}
