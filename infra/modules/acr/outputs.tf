output "acr_login_server" {
  description = "The login server name for ACR (e.g. 'myregistry.azurecr.io')."
  value       = azurerm_container_registry.acr.login_server
}

output "acr_id" {
  description = "The resource ID of the Azure Container Registry."
  value       = azurerm_container_registry.acr.id
}

output "login_server" {
  value = azurerm_container_registry.acr.login_server
}
