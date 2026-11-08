output "kube_config" {
  value     = azurerm_kubernetes_cluster.aks.kube_config_raw
  sensitive = true
}

output "kubelet_identity" {
  description = "The kubelet identity used by AKS to pull from ACR."
  value = {
    object_id = azurerm_kubernetes_cluster.aks.kubelet_identity[0].object_id
    client_id = azurerm_kubernetes_cluster.aks.kubelet_identity[0].client_id
  }
}
