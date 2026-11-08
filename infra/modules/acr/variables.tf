variable "resource_group_name" {
  description = "The name of the existing resource group."
  type        = string
}

variable "location" {
  description = "The Azure region to deploy ACR."
  type        = string
}

variable "acr_name" {
  description = "Name of the Azure Container Registry (must be globally unique)."
  type        = string
}

variable "kubelet_identity" {
  description = "Kubelet identity of the AKS cluster"
  type = object({
    object_id = string
    client_id = string
  })
}
