Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ============================================================
# connect_to_aks.ps1
#
# Logs in to Azure with a Service Principal, selects the target
# subscription, fetches AKS credentials, and does a quick health
# check (pods, services, Grafana admin password).
#
# All identifiers below are placeholders — set them for your own
# Azure environment (or export them as environment variables).
# ============================================================

function Assert-LastExitCode {
    param(
        [string]$StepName
    )

    if ($LASTEXITCODE -ne 0) {
        Write-Error "$StepName failed (exit code $LASTEXITCODE). Exiting."
        exit $LASTEXITCODE
    }
}

# ----------------------------------------------------------
# Target environment (placeholders — replace for your setup).
# ----------------------------------------------------------
$resourceGroup  = "rg-lakehouse-on-k8s-01"
$aksName        = "aks-lakehouse-on-k8s-01"
$subscriptionId = $env:AZURE_SUBSCRIPTION_ID   # e.g. "00000000-0000-0000-0000-000000000000"

if ([string]::IsNullOrWhiteSpace($subscriptionId)) {
    $subscriptionId = Read-Host "Enter the Azure Subscription ID"
}

# Prompt for credentials
$clientId = Read-Host "Enter the Service Principal Client ID"
$clientSecret = Read-Host "Enter the Client Secret" -AsSecureString
$secretBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($clientSecret)
$plainSecret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($secretBstr)
$tenantId = Read-Host "Enter the Tenant ID"

try {
    # Login using the service principal
    Write-Host "Authenticating with Azure using Service Principal..."
    az login --service-principal `
             --username $clientId `
             --password $plainSecret `
             --tenant $tenantId `
             --output none
    Assert-LastExitCode "Azure login"

    Write-Host "Selecting subscription..."
    az account set --subscription $subscriptionId
    Assert-LastExitCode "Azure subscription selection"

    $activeSubscription = (az account show --query id --output tsv).Trim()
    Assert-LastExitCode "Azure active subscription verification"
    if ($activeSubscription -ne $subscriptionId) {
        Write-Error "Active subscription mismatch. Expected '$subscriptionId' but got '$activeSubscription'. Exiting."
        exit 1
    }

    $groupExists = (az group exists --name $resourceGroup --output tsv).Trim()
    Assert-LastExitCode "Resource group existence check"
    if ($groupExists -ne "true") {
        Write-Error "Resource group '$resourceGroup' was not found in subscription '$subscriptionId'. Exiting."
        exit 1
    }

    # Set AKS credentials
    Write-Host "Fetching AKS credentials for $aksName..."
    az aks get-credentials --resource-group $resourceGroup --name $aksName --overwrite-existing
    Assert-LastExitCode "AKS credential fetch"

    # Verify access to the cluster
    Write-Host "Getting pods from all namespaces..."
    kubectl get pods -A
    Assert-LastExitCode "kubectl get pods"

    Write-Host "Getting services from all namespaces..."
    kubectl get svc -A
    Assert-LastExitCode "kubectl get services"

    Write-Host "Retrieving Grafana admin password..."
    kubectl -n monitoring get secret kps-grafana -o jsonpath="{.data.admin-password}" | ForEach-Object { [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_)) }
    Assert-LastExitCode "Grafana password retrieval"
}
finally {
    if ($secretBstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretBstr)
    }
}
