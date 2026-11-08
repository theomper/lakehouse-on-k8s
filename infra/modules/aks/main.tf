resource "azurerm_kubernetes_cluster" "aks" {
  name                = var.cluster_name
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = "${var.cluster_name}-dns"

  default_node_pool {
    name                = var.system_node_pool.name
    node_count          = var.system_node_pool.node_count
    vm_size             = var.system_node_pool.vm_size
    enable_auto_scaling = var.system_node_pool.enable_auto_scaling
    min_count           = var.system_node_pool.min_count
    max_count           = var.system_node_pool.max_count

    # Recommended settings for system pool
    type           = "VirtualMachineScaleSets"
    vnet_subnet_id = null # Set if using custom VNET
    zones          = []   # Set availability zones if needed
    node_labels    = {}
    tags           = {}
  }

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "user" {
  name                  = var.user_node_pool.name
  kubernetes_cluster_id = azurerm_kubernetes_cluster.aks.id
  vm_size               = var.user_node_pool.vm_size
  node_count            = var.user_node_pool.node_count
  enable_auto_scaling   = var.user_node_pool.enable_auto_scaling
  min_count             = var.user_node_pool.min_count
  max_count             = var.user_node_pool.max_count

  # Recommended settings for user pool
  mode     = "User"
  os_type  = "Linux"
  priority = "Regular" # Set to "Spot" for cost savings if desired
  node_labels = {
    "nodepool-type" = "user"
  }

  node_taints = [] # You could add taints here if needed
}