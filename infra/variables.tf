variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "cluster_name" {
  description = "AKS cluster name"
  type        = string
  default     = "lakehouse-aks"
}

variable "system_node_count" {
  description = "Initial number of nodes in the system node pool"
  type        = number
}

variable "system_vm_size" {
  description = "VM size for system node pool"
  type        = string
}

variable "system_min_nodes" {
  description = "Minimum number of nodes for system pool"
  type        = number
}

variable "system_max_nodes" {
  description = "Maximum number of nodes for system pool"
  type        = number
}

variable "user_node_count" {
  description = "Initial number of nodes in the user node pool"
  type        = number
}

variable "user_vm_size" {
  description = "VM size for user node pool"
  type        = string
}

variable "user_min_nodes" {
  description = "Minimum number of nodes for user pool"
  type        = number
}

variable "user_max_nodes" {
  description = "Maximum number of nodes for user pool"
  type        = number
}

variable "storage_account_name" {
  description = "Name for the ADLS Gen2 storage account (globally unique)."
  type        = string
}

variable "acr_name" {
  description = "Name for the Azure Container Registry (globally unique)."
  type        = string
}

# VM configuration
variable "vm_name" {
  description = "Name for the Windows experiment VM"
  type        = string
  default     = "vm-lakehouse-01"
}

variable "vm_size" {
  description = "Azure VM size for the experiment VM"
  type        = string
  default     = "Standard_B2s"
}

variable "vm_admin_username" {
  description = "Administrator username for the Windows VM"
  type        = string
  default     = "azureadmin"
}

variable "vm_admin_password" {
  description = "Administrator password for the Windows VM"
  type        = string
  sensitive   = true
}
