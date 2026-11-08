variable "resource_group_name" {
  type        = string
  description = "Name of the Azure Resource Group"
}

variable "location" {
  type        = string
  description = "Azure region where resources will be deployed"
}

variable "vm_name" {
  type        = string
  description = "Name of the Windows Virtual Machine"
}

variable "vm_size" {
  type        = string
  description = "Azure VM size (e.g. Standard_B2s for 2 vCPUs / 4 GB)"
  default     = "Standard_B2s"
}

variable "admin_username" {
  type        = string
  description = "Administrator username for the Windows VM"
}

variable "admin_password" {
  type        = string
  description = "Administrator password for the Windows VM"
  sensitive   = true
}

variable "vnet_address_space" {
  type        = string
  description = "Address space for the virtual network"
  default     = "10.1.0.0/16"
}

variable "subnet_address_prefix" {
  type        = string
  description = "Address prefix for the VM subnet"
  default     = "10.1.1.0/24"
}
