# Project Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "hiscox-etl"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Azure Configuration
variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-hiscox-etl-dev"
}

# Storage Configuration
variable "storage_account_name" {
  description = "Name of the Azure Storage Account (must be globally unique)"
  type        = string
  default     = "sthiscoxetldev001"
  
  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "Storage account name must be between 3 and 24 characters, lowercase letters and numbers only."
  }
}

# Key Vault Configuration
variable "key_vault_name" {
  description = "Name of the Azure Key Vault (must be globally unique)"
  type        = string
  default     = "kv-hiscox-etl-dev-001"
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9-]{3,24}$", var.key_vault_name))
    error_message = "Key Vault name must be between 3 and 24 characters, alphanumeric and hyphens only."
  }
}

# Databricks Configuration
variable "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
  default     = "dbw-hiscox-etl-dev"
}

variable "databricks_sku" {
  description = "SKU for Databricks workspace"
  type        = string
  default     = "standard"
  
  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "Databricks SKU must be one of: standard, premium, trial."
  }
}

# Container Registry Configuration
variable "container_registry_name" {
  description = "Name of the Azure Container Registry (must be globally unique)"
  type        = string
  default     = "acrhiscoxetldev001"
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9]{5,50}$", var.container_registry_name))
    error_message = "Container Registry name must be between 5 and 50 characters, alphanumeric only."
  }
}

# Network Configuration
variable "allowed_ip_ranges" {
  description = "List of IP ranges allowed to access resources"
  type        = list(string)
  default     = ["0.0.0.0/0"] # In production, restrict this to specific IP ranges
}

# Tags
variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "Hiscox ETL Pipeline"
    Environment = "Development"
    Owner       = "Data Engineering Team"
    CostCenter  = "IT-DataEngineering"
    CreatedBy   = "Terraform"
  }
}

# Databricks Cluster Configuration
variable "cluster_node_type" {
  description = "Node type for Databricks cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "cluster_min_workers" {
  description = "Minimum number of workers in Databricks cluster"
  type        = number
  default     = 1
}

variable "cluster_max_workers" {
  description = "Maximum number of workers in Databricks cluster"
  type        = number
  default     = 4
}

variable "cluster_autotermination_minutes" {
  description = "Auto-termination time for Databricks cluster in minutes"
  type        = number
  default     = 20
}

# Data Configuration
variable "data_retention_days" {
  description = "Number of days to retain data in storage"
  type        = number
  default     = 90
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
}

# Security Configuration
variable "enable_soft_delete" {
  description = "Enable soft delete for Key Vault"
  type        = bool
  default     = true
}

variable "purge_protection_enabled" {
  description = "Enable purge protection for Key Vault"
  type        = bool
  default     = false # Set to true for production
}
