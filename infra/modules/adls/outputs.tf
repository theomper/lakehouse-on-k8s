output "storage_account_id" {
  description = "The ID of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.adls.id
}

output "storage_account_name" {
  description = "The name of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.adls.name
}

output "filesystem_warehouse" {
  description = "Name of the 'warehouse' filesystem in ADLS Gen2."
  value       = azurerm_storage_data_lake_gen2_filesystem.warehouse.name
}


output "filesystem_checkpoints" {
  description = "Name of the 'checkpoints' filesystem in ADLS Gen2."
  value       = azurerm_storage_data_lake_gen2_filesystem.checkpoints.name
}