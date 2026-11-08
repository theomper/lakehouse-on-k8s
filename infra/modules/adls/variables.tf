variable "resource_group_name" {
  description = "The name of the existing resource group."
  type        = string
}

variable "location" {
  description = "The Azure region to deploy ADLS."
  type        = string
}

variable "storage_account_name" {
  description = "Name of the Storage Account (must be globally unique, 3–24 lowercase letters and numbers)."
  type        = string
}
