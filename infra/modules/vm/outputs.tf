output "vm_id" {
  description = "The resource ID of the Windows VM"
  value       = azurerm_windows_virtual_machine.vm.id
}

output "vm_name" {
  description = "The name of the Windows VM"
  value       = azurerm_windows_virtual_machine.vm.name
}

output "public_ip_address" {
  description = "The public IP address assigned to the VM"
  value       = azurerm_public_ip.vm_pip.ip_address
}

output "private_ip_address" {
  description = "The private IP address assigned to the VM"
  value       = azurerm_network_interface.vm_nic.private_ip_address
}
