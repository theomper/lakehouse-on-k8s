# Terraform variables for Azure resources (placeholder names, replace before deploying)
resource_group_name = "rg-lakehouse-on-k8s-01"
location            = "westeurope"
cluster_name        = "aks-lakehouse-on-k8s-01"

# System node pool configuration (1 node fixed)
system_node_count = 1
system_vm_size    = "Standard_D2s_v6"
system_min_nodes  = 1
system_max_nodes  = 1

# User node pool configuration (2 nodes fixed)
user_node_count = 2
user_vm_size    = "Standard_E8s_v6"
user_min_nodes  = 2
user_max_nodes  = 2

storage_account_name = "stlakehouseonk8s01"  # must be 3–24 lowercase, no special chars
acr_name             = "acrlakehouseonk8s01" # must be globally unique, lowercase

# Standard_B2s is capacity-constrained in westeurope; D2s_v3 is the available equivalent
vm_size = "Standard_D2s_v6"
vm_name = "vm-lakehouse-01"
