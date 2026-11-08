# infra-destroy.ps1
# Local equivalent of .github/workflows/infra-destroy.yml
# Deletes the entire resource group (and all resources within it).
# Naming convention: "lakehouse-on-k8s" variant (matches deploy-azcli.ps1).
# Prerequisites: az login.
# Usage: .\infra-destroy.ps1

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------
# Naming convention — "lakehouse-on-k8s" variant
# ---------------------------------------------------------------
$resourceGroup = "rg-lakehouse-on-k8s-01"

# ---------------------------------------------------------------
# Confirm before deleting
# ---------------------------------------------------------------
Write-Host "This will DELETE resource group '$resourceGroup' and ALL resources inside it." -ForegroundColor Red
$confirm = Read-Host "Type the resource group name to confirm"

if ($confirm -ne $resourceGroup) {
    Write-Host "Confirmation did not match. Aborting." -ForegroundColor Yellow
    exit 1
}

# ---------------------------------------------------------------
# Delete Resource Group
# ---------------------------------------------------------------
Write-Host "`nDeleting resource group $resourceGroup..." -ForegroundColor Cyan
az group delete --name $resourceGroup --yes --no-wait

Write-Host "Resource group deletion initiated (--no-wait). It may take several minutes to complete." -ForegroundColor Green
