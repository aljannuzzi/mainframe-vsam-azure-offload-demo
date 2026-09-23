param(
    [Parameter(Mandatory = $true)]
    [string]$Subscription,
    [string]$ResourceGroup = "rg-mainframe-vsam-offload-demo",
    [string]$Location = "brazilsouth",
    [string]$ImageTag = ("demo-" + (Get-Date -Format "yyyyMMddHHmmss")),
    [switch]$InfrastructureOnly
)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
try {
    $json = az deployment group show --subscription $Subscription --resource-group $ResourceGroup `
        --name main --query properties.outputs --output json
    if ($LASTEXITCODE -ne 0) { throw "Cannot read the existing base deployment." }
    $base = $json | ConvertFrom-Json
    foreach ($name in @("storageAccountName", "cosmosEndpoint", "eventHubNamespace", "eventHubName")) {
        if (-not $base.$name.value) { throw "Base deployment is missing output $name." }
    }

    New-Item -ItemType Directory -Force -Path ".\out" | Out-Null
    $accessFile = Join-Path $root "out\guided-access.clixml"
    if (Test-Path $accessFile) {
        $secureToken = Import-Clixml -LiteralPath $accessFile
    }
    else {
        $bytes = New-Object byte[] 32
        $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        $secureToken = ConvertTo-SecureString ([Convert]::ToBase64String($bytes)) -AsPlainText -Force
        $secureToken | Export-Clixml -LiteralPath $accessFile
    }
    $accessToken = (New-Object Net.NetworkCredential("", $secureToken)).Password
    $cosmosName = ([Uri]$base.cosmosEndpoint.value).Host.Split(".")[0]
    $parameters = @(
        "location=$Location",
        "storageAccountName=$($base.storageAccountName.value)",
        "cosmosAccountName=$cosmosName",
        "eventHubNamespaceName=$($base.eventHubNamespace.value)",
        "eventHubName=$($base.eventHubName.value)",
        "demoAccessToken=$accessToken",
        "deployApplication=false"
    )

    $infraJson = az deployment group create --subscription $Subscription --resource-group $ResourceGroup `
        --name guided-demo --template-file ".\infra\guided-app.bicep" --parameters $parameters `
        --only-show-errors --query properties.outputs --output json
    if ($LASTEXITCODE -ne 0) { throw "Guided infrastructure deployment failed." }
    $infra = $infraJson | ConvertFrom-Json
    if (-not $infra.registryName.value) { throw "Registry output missing." }

    if (-not $InfrastructureOnly) {
        az acr build --subscription $Subscription --registry $infra.registryName.value `
            --image "vsam-guided:$ImageTag" --file Dockerfile --timeout 1200 --no-logs --output none .
        if ($LASTEXITCODE -ne 0) { throw "Remote container image build failed." }

        $parameters = $parameters | Where-Object { $_ -ne "deployApplication=false" }
        $parameters += @("deployApplication=true", "imageTag=$ImageTag")
        $appJson = az deployment group create --subscription $Subscription --resource-group $ResourceGroup `
            --name guided-demo --template-file ".\infra\guided-app.bicep" --parameters $parameters `
            --only-show-errors --query properties.outputs --output json
        if ($LASTEXITCODE -ne 0) { throw "Guided application deployment failed." }
        $app = $appJson | ConvertFrom-Json
        Write-Host "Web application: $($app.webUrl.value)"
    }
    Write-Host "Access code is stored with Windows DPAPI at $accessFile"
    Write-Host "Do not commit or share the access file. Keep data service public access disabled."
}
finally {
    $accessToken = $null
    Pop-Location
}
