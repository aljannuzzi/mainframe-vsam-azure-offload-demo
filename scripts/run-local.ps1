param(
    [string]$Output = ("out\sync-" + (Get-Date -Format "yyyyMMdd-HHmmss")),
    [double]$IntervalSeconds = 0.2
)

$ErrorActionPreference = "Stop"

Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    python -m vsam_offload.sync_demo --output $Output --interval-seconds $IntervalSeconds
    if ($LASTEXITCODE -ne 0) {
        throw "Local synchronization demo failed (exit $LASTEXITCODE)."
    }
}
finally {
    Pop-Location
}
