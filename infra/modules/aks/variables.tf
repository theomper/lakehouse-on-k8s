variable "resource_group_name" {
  type        = string
  description = "Name of the Azure Resource Group"
}

variable "location" {
  type        = string
  description = "Azure region where resources will be deployed"
}

variable "cluster_name" {
  type        = string
  description = "AKS cluster name"
}

variable "system_node_pool" {
  description = "Configuration for the system node pool"
  type = object({
    name                = string
    node_count          = number
    vm_size             = string
    min_count           = number
    max_count           = number
    enable_auto_scaling = bool
  })
}

variable "user_node_pool" {
  description = "Configuration for the user node pool"
  type = object({
    name                = string
    node_count          = number
    vm_size             = string
    min_count           = number
    max_count           = number
    enable_auto_scaling = bool
  })
}
