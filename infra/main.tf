provider "azurerm" {
  features {}
  skip_provider_registration = true
}

data "azurerm_resource_group" "rg" {
  name = var.resource_group_name
}

module "aks" {
  source              = "./modules/aks"
  resource_group_name = var.resource_group_name
  cluster_name        = var.cluster_name
  location            = var.location

  # System node pool configuration
  system_node_pool = {
    name                = "systempool"
    node_count          = var.system_node_count
    vm_size             = var.system_vm_size
    min_count           = var.system_min_nodes
    max_count           = var.system_max_nodes
    enable_auto_scaling = true
  }

  # User node pool configuration
  user_node_pool = {
    name                = "userpool"
    node_count          = var.user_node_count
    vm_size             = var.user_vm_size
    min_count           = var.user_min_nodes
    max_count           = var.user_max_nodes
    enable_auto_scaling = true
  }
}

module "adls" {
  source               = "./modules/adls"
  resource_group_name  = var.resource_group_name
  location             = var.location
  storage_account_name = var.storage_account_name
}

module "acr" {
  source              = "./modules/acr"
  resource_group_name = var.resource_group_name
  location            = var.location
  acr_name            = var.acr_name
  kubelet_identity    = module.aks.kubelet_identity
}

module "vm" {
  source              = "./modules/vm"
  resource_group_name = var.resource_group_name
  location            = var.location
  vm_name             = var.vm_name
  vm_size             = var.vm_size
  admin_username      = var.vm_admin_username
  admin_password      = var.vm_admin_password
}