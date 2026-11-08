# trino-paimon-image.ps1
# Local equivalent of .github/workflows/trino-paimon-image.yml
# Builds and pushes the Trino + Paimon image to ACR.
# Naming convention: "lakehouse-on-k8s" variant (matches deploy-azcli.ps1).
# Uses ACR Tasks (az acr build) — Docker does NOT need to be installed locally.
# Prerequisites: az login
# Usage: .\trino-paimon-image.ps1

$ErrorActionPreference = "Stop"

# Ensure all relative paths resolve from the repo root
Push-Location (Join-Path $PSScriptRoot "..\..")
try {

# ---------------------------------------------------------------
# Naming convention — "lakehouse-on-k8s" variant
# ---------------------------------------------------------------
$acrName        = "acrlakehouseonk8s01"
$acrLoginServer = "$acrName.azurecr.io"
$imageName      = "trino-paimon"
$trinoVersion   = "440"

# ---------------------------------------------------------------
# Build and push Trino + Paimon image (ACR Task — no local Docker needed)
# ---------------------------------------------------------------
Write-Host "`nBuilding Trino Paimon image (Trino $trinoVersion) via ACR Task..." -ForegroundColor Cyan

az acr build `
    --registry $acrName `
    --image "${imageName}:$trinoVersion" `
    --image "${imageName}:latest" `
    --build-arg TRINO_VERSION=$trinoVersion `
    --file docker/trino-paimon.Dockerfile `
    docker

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
Write-Host "`n===== Trino Paimon Image Pushed =====" -ForegroundColor Green
Write-Host "  $acrLoginServer/${imageName}:$trinoVersion"
Write-Host "  $acrLoginServer/${imageName}:latest"

} finally { Pop-Location }
