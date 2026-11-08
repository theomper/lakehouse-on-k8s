# producer-ci.ps1
# Local equivalent of .github/workflows/producer-ci.yml
# Builds and pushes the three IoT simulated data producer images to ACR.
# Naming convention: "lakehouse-on-k8s" variant (matches deploy-azcli.ps1).
# Uses ACR Tasks (az acr build) — Docker does NOT need to be installed locally.
# Prerequisites: az login
# Usage: .\producer-ci.ps1

$ErrorActionPreference = "Stop"

# Ensure all relative paths resolve from the repo root
Push-Location (Join-Path $PSScriptRoot "..\..")
try {

# ---------------------------------------------------------------
# Naming convention — "lakehouse-on-k8s" variant
# ---------------------------------------------------------------
$acrName        = "acrlakehouseonk8s01"
$acrLoginServer = "$acrName.azurecr.io"

# Image names (match the CI workflow)
$pythonImage  = "iot_sim_data_producer"
$goImage      = "go_iot_sim_data_producer"
$goBatchImage = "go_iot_sim_data_producer_batched"

# ---------------------------------------------------------------
# Build & push Python producer (ACR Task — no local Docker needed)
# ---------------------------------------------------------------
Write-Host "`nBuilding Python producer image via ACR Task..." -ForegroundColor Cyan
az acr build `
    --registry $acrName `
    --image "${pythonImage}:latest" `
    --file docker/iot-sim-producer.Dockerfile `
    .

# ---------------------------------------------------------------
# Build & push Go producer (ACR Task — no local Docker needed)
# ---------------------------------------------------------------
Write-Host "`nBuilding Go producer image via ACR Task..." -ForegroundColor Cyan
az acr build `
    --registry $acrName `
    --image "${goImage}:latest" `
    --file docker/go-iot-sim-producer.Dockerfile `
    .

# ---------------------------------------------------------------
# Build & push Go batched producer (ACR Task — no local Docker needed)
# ---------------------------------------------------------------
Write-Host "`nBuilding Go batched producer image via ACR Task..." -ForegroundColor Cyan
az acr build `
    --registry $acrName `
    --image "${goBatchImage}:latest" `
    --file docker/go-iot-sim-producer-batched.Dockerfile `
    .

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
Write-Host "`n===== Producer Images Pushed =====" -ForegroundColor Green
Write-Host "  $acrLoginServer/${pythonImage}:latest"
Write-Host "  $acrLoginServer/${goImage}:latest"
Write-Host "  $acrLoginServer/${goBatchImage}:latest"

} finally { Pop-Location }
