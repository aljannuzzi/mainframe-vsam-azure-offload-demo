param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,

    [string]$Location = "brazilsouth",

    [string]$EnvironmentName = "mfvsamdemo"
)

$ErrorActionPreference = "Stop"

az group create --name $ResourceGroup --location $Location --output table
if ($LASTEXITCODE -ne 0) { throw "Resource group creation failed." }
$templatePath = Join-Path (Split-Path $PSScriptRoot -Parent) "infra\main.bicep"
az deployment group create `
    --name main `
    --resource-group $ResourceGroup `
    --template-file $templatePath `
    --parameters environmentName=$EnvironmentName `
    --output table
if ($LASTEXITCODE -ne 0) { throw "Azure deployment failed." }
