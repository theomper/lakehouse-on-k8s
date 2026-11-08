# deploy-azcli.ps1
# Deploys the full lakehouse-on-k8s infrastructure using Azure CLI.
# Resource names below are placeholders that match infra/terraform.tfvars — replace with your own.
# Usage: .\deploy-azcli.ps1 -VmAdminPassword '<password>'
# Prerequisites: Azure CLI >= 2.67 (run 'az upgrade' if Bastion Developer SKU is unsupported).
#   Always quote the password in single quotes to avoid shell interpretation of special chars.

param(
    [Parameter(Mandatory = $true)]
    [string]$VmAdminPassword
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------
# Naming convention — "lakehouse-on-k8s" variant
# ---------------------------------------------------------------
$location           = "swedencentral"
$resourceGroup      = "rg-lakehouse-on-k8s-01"
$aksCluster         = "aks-lakehouse-on-k8s-01"
$storageAccount     = "stlakehouseonk8s01"
$acrName            = "acrlakehouseonk8s01"
$vmName             = "vm-lakehouse-01"

# AKS node pool sizes (match terraform.secondary.tfvars active SKUs)
$systemVmSize       = "Standard_D2s_v6"
$systemNodeCount    = 1
$userVmSize         = "Standard_E8s_v6"
$userNodeCount      = 2

# VM settings
$vmSize             = "Standard_B2ms"
$vmAdminUser        = "azureadmin"
$vmImage            = "MicrosoftWindowsServer:WindowsServer:2025-datacenter-azure-edition:latest"

# Networking for the VM
$vnetName           = "vnet-$vmName"
$subnetName         = "snet-$vmName"
$nsgName            = "nsg-$vmName"
$nicName            = "nic-$vmName"
$bastionName        = "bas-$vmName"

# AKS networking — pre-created VNet to comply with NSG-on-subnet policy
$aksVnetName        = "vnet-$aksCluster"
$aksSubnetName      = "snet-$aksCluster"
$aksNsgName         = "nsg-$aksCluster"

# ---------------------------------------------------------------
# 1. Resource Group
# ---------------------------------------------------------------
Write-Host "Creating resource group $resourceGroup..." -ForegroundColor Cyan
az group create --name $resourceGroup --location $location --output none

# ---------------------------------------------------------------
# 2. AKS Networking (VNet + NSG — required by org policy)
# ---------------------------------------------------------------
Write-Host "Creating NSG $aksNsgName for AKS subnet..." -ForegroundColor Cyan
az network nsg create `
    --resource-group $resourceGroup `
    --name $aksNsgName `
    --location $location `
    --output none

Write-Host "Creating VNet $aksVnetName with NSG-attached subnet..." -ForegroundColor Cyan
az network vnet create `
    --resource-group $resourceGroup `
    --name $aksVnetName `
    --location $location `
    --address-prefix 10.0.0.0/16 `
    --subnet-name $aksSubnetName `
    --subnet-prefix 10.0.0.0/22 `
    --network-security-group $aksNsgName `
    --output none

$aksSubnetId = (az network vnet subnet show `
    --resource-group $resourceGroup `
    --vnet-name $aksVnetName `
    --name $aksSubnetName `
    --query "id" -o tsv)

# ---------------------------------------------------------------
# 3. AKS Cluster (system pool created with the cluster)
# ---------------------------------------------------------------
Write-Host "Creating AKS cluster $aksCluster..." -ForegroundColor Cyan
az aks create `
    --resource-group $resourceGroup `
    --name $aksCluster `
    --location $location `
    --node-count $systemNodeCount `
    --node-vm-size $systemVmSize `
    --nodepool-name systempool `
    --vnet-subnet-id $aksSubnetId `
    --service-cidr 172.16.0.0/16 `
    --dns-service-ip 172.16.0.10 `
    --enable-cluster-autoscaler `
    --min-count $systemNodeCount `
    --max-count $systemNodeCount `
    --generate-ssh-keys `
    --output none

# Add user node pool
Write-Host "Adding user node pool..." -ForegroundColor Cyan
az aks nodepool add `
    --resource-group $resourceGroup `
    --cluster-name $aksCluster `
    --name userpool `
    --node-count $userNodeCount `
    --node-vm-size $userVmSize `
    --mode User `
    --vnet-subnet-id $aksSubnetId `
    --enable-cluster-autoscaler `
    --min-count $userNodeCount `
    --max-count $userNodeCount `
    --labels nodepool-type=user `
    --output none

# ---------------------------------------------------------------
# 4. ADLS Gen2 Storage Account + filesystems
# ---------------------------------------------------------------
Write-Host "Creating ADLS Gen2 storage account $storageAccount..." -ForegroundColor Cyan
az storage account create `
    --name $storageAccount `
    --resource-group $resourceGroup `
    --location $location `
    --sku Standard_LRS `
    --kind StorageV2 `
    --hns true `
    --https-only true `
    --tags environment=dev `
    --output none

Write-Host "Creating filesystems (warehouse, checkpoints)..." -ForegroundColor Cyan
$storageKey = (az storage account keys list --resource-group $resourceGroup --account-name $storageAccount --query "[0].value" -o tsv)

az storage fs create --name warehouse    --account-name $storageAccount --account-key $storageKey --output none
az storage fs create --name checkpoints  --account-name $storageAccount --account-key $storageKey --output none

# ---------------------------------------------------------------
# 5. Azure Container Registry + AcrPull role for AKS
# ---------------------------------------------------------------
Write-Host "Creating ACR $acrName..." -ForegroundColor Cyan
az acr create `
    --resource-group $resourceGroup `
    --name $acrName `
    --sku Standard `
    --location $location `
    --output none

# Grant AKS kubelet identity AcrPull on the registry
Write-Host "Assigning AcrPull role to AKS kubelet identity..." -ForegroundColor Cyan
$kubeletObjectId = (az aks show --resource-group $resourceGroup --name $aksCluster --query "identityProfile.kubeletidentity.objectId" -o tsv)
$acrId           = (az acr show --resource-group $resourceGroup --name $acrName --query "id" -o tsv)

az role assignment create `
    --assignee-object-id $kubeletObjectId `
    --assignee-principal-type ServicePrincipal `
    --role AcrPull `
    --scope $acrId `
    --output none

# ---------------------------------------------------------------
# 6. Windows VM (experiment workstation)
# ---------------------------------------------------------------

# NSG must be created before VNet (policy requires NSG on subnets)
Write-Host "Creating NSG $nsgName..." -ForegroundColor Cyan
az network nsg create `
    --resource-group $resourceGroup `
    --name $nsgName `
    --location $location `
    --output none

Write-Host "Creating VNet $vnetName (with NSG on subnet)..." -ForegroundColor Cyan
az network vnet create `
    --resource-group $resourceGroup `
    --name $vnetName `
    --location $location `
    --address-prefix 10.1.0.0/16 `
    --subnet-name $subnetName `
    --subnet-prefix 10.1.1.0/24 `
    --network-security-group $nsgName `
    --output none

Write-Host "Creating NIC $nicName (no public IP - access via Bastion)..." -ForegroundColor Cyan
az network nic create `
    --resource-group $resourceGroup `
    --name $nicName `
    --location $location `
    --vnet-name $vnetName `
    --subnet $subnetName `
    --network-security-group $nsgName `
    --output none

Write-Host "Creating Bastion $bastionName (Developer SKU)..." -ForegroundColor Cyan
az network bastion create `
    --resource-group $resourceGroup `
    --name $bastionName `
    --vnet-name $vnetName `
    --sku Developer `
    --location $location `
    --output none

Write-Host "Creating Windows VM $vmName..." -ForegroundColor Cyan
az vm create `
    --resource-group $resourceGroup `
    --name $vmName `
    --location $location `
    --size $vmSize `
    --image $vmImage `
    --admin-username $vmAdminUser `
    --admin-password "`"$VmAdminPassword`"" `
    --nics $nicName `
    --os-disk-name "osdisk-$vmName" `
    --storage-sku Standard_LRS `
    --output none

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
$acrLoginServer = (az acr show --resource-group $resourceGroup --name $acrName --query "loginServer" -o tsv)

Write-Host "`n===== Deployment Complete =====" -ForegroundColor Green
Write-Host "Resource Group   : $resourceGroup"
Write-Host "AKS Cluster      : $aksCluster"
Write-Host "Storage Account  : $storageAccount"
Write-Host "ACR Login Server : $acrLoginServer"
Write-Host "VM Name          : $vmName"
Write-Host "VM Admin User    : $vmAdminUser"
Write-Host "Bastion          : $bastionName"
Write-Host "`nConnect via Bastion (Developer SKU - use Azure Portal):" -ForegroundColor Yellow
Write-Host "  Go to VM '$vmName' in Azure Portal -> Connect -> Bastion" -ForegroundColor Yellow
Write-Host "Then run scripts\setup-vm.ps1 on the VM to install VS Code, Git, and Azure CLI." -ForegroundColor Yellow
