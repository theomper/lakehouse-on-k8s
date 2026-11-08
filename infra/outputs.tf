output "kube_config" {
  description = "Kubeconfig JSON for accessing the AKS cluster"
  value       = module.aks.kube_config
  sensitive   = true
}

output "storage_account_id" {
  description = "The ID of the ADLS Gen2 storage account."
  value       = module.adls.storage_account_id
}

output "storage_account_name" {
  description = "The name of the ADLS Gen2 storage account."
  value       = module.adls.storage_account_name
}

output "filesystem_warehouse" {
  description = "Name of the 'warehouse' filesystem in ADLS Gen2."
  value       = module.adls.filesystem_warehouse
}

output "filesystem_checkpoints" {
  description = "Name of the 'checkpoints' filesystem in ADLS Gen2."
  value       = module.adls.filesystem_checkpoints
}

output "acr_login_server" {
  description = "The login server name for ACR (e.g. myregistry.azurecr.io)."
  value       = module.acr.acr_login_server
}

output "acr_id" {
  description = "The resource ID of the Azure Container Registry."
  value       = module.acr.acr_id
}

output "vm_public_ip" {
  description = "The public IP address of the experiment VM."
  value       = module.vm.public_ip_address
}

output "vm_name" {
  description = "The name of the experiment VM."
  value       = module.vm.vm_name
}