$ErrorActionPreference = "Stop"
$path = Join-Path (Split-Path $PSScriptRoot -Parent) "out\guided-access.clixml"
if (-not (Test-Path $path)) { throw "Run deploy-guided.ps1 on this Windows account first." }
$secure = Import-Clixml -LiteralPath $path
Write-Host "Private demo access code (do not include in screenshots or recordings):"
Write-Output (New-Object Net.NetworkCredential("", $secure)).Password
