# Production Environment Terraform Configuration
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.0"
    }
  }
  
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstateprod001"
    container_name       = "tfstate"
    key                  = "hiscox-etl-prod.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}

provider "azuread" {}

# Variables
variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "is_primary" {
  description = "Whether this is the primary region"
  type        = bool
  default     = true
}

# Data sources
data "azurerm_client_config" "current" {}

# Local variables
locals {
  environment = "prod"
  location    = var.location
  region_suffix = var.location == "East US" ? "001" : var.location == "West US 2" ? "002" : "003"
  
  common_tags = {
    Environment = local.environment
    Project     = "hiscox-etl"
    ManagedBy   = "terraform"
    Owner       = "data-engineering"
    CostCenter  = "data-engineering"
    Compliance  = "required"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "rg-hiscox-etl-${local.environment}-${local.region_suffix}"
  location = local.location
  tags     = local.common_tags
}

# Storage Account with production settings
resource "azurerm_storage_account" "main" {
  name                     = "sthiscoxetl${local.environment}${local.region_suffix}am"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.is_primary ? "GRS" : "LRS"
  
  # Production security settings
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  
  # Network rules for production
  network_rules {
    default_action = "Deny"
    ip_rules       = ["10.0.0.0/8", "172.16.0.0/12"]  # Corporate IP ranges
    bypass         = ["AzureServices"]
  }
  
  blob_properties {
    delete_retention_policy {
      days = 90
    }
    container_delete_retention_policy {
      days = 90
    }
    versioning_enabled = true
  }
  
  tags = local.common_tags
}

# Storage Containers
resource "azurerm_storage_container" "raw_data" {
  name                  = "raw-data"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed_data" {
  name                  = "processed-data"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "logs" {
  name                  = "logs"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "archive" {
  name                  = "archive"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Key Vault with production settings
resource "azurerm_key_vault" "main" {
  name                = "kv-hiscox-etl-${local.environment}-${local.region_suffix}am"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "premium"
  
  # Production settings
  purge_protection_enabled   = true
  soft_delete_retention_days = 90
  
  # Network ACLs for production
  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = ["10.0.0.0/8", "172.16.0.0/12"]
  }
  
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    
    key_permissions = [
      "Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore"
    ]
    
    secret_permissions = [
      "Get", "List", "Set", "Delete", "Recover", "Backup", "Restore"
    ]
  }
  
  tags = local.common_tags
}

# Container Registry with production settings
resource "azurerm_container_registry" "main" {
  name                = "acrhiscoxetl${local.environment}${local.region_suffix}am"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Premium"
  admin_enabled       = false  # Use managed identity in production
  
  # Production features
  quarantine_policy_enabled = true
  trust_policy_enabled      = true
  retention_policy_enabled  = true
  
  network_rule_set {
    default_action = "Deny"
    ip_rule {
      action   = "Allow"
      ip_range = "10.0.0.0/8"
    }
    ip_rule {
      action   = "Allow"
      ip_range = "172.16.0.0/12"
    }
  }
  
  tags = local.common_tags
}

# Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-hiscox-etl-${local.environment}-${local.region_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 90
  
  tags = local.common_tags
}

# Databricks Workspace with premium features
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-hiscox-etl-${local.environment}-${local.region_suffix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"
  
  # Production network settings
  custom_parameters {
    no_public_ip        = true
    public_subnet_name  = azurerm_subnet.databricks_public.name
    private_subnet_name = azurerm_subnet.databricks_private.name
    virtual_network_id  = azurerm_virtual_network.main.id
  }
  
  tags = local.common_tags
}

# Virtual Network for production
resource "azurerm_virtual_network" "main" {
  name                = "vnet-hiscox-etl-${local.environment}-${local.region_suffix}"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  tags = local.common_tags
}

# Subnets for Databricks
resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]
  
  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.2.0/24"]
  
  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

# Application Insights
resource "azurerm_application_insights" "main" {
  name                = "ai-hiscox-etl-${local.environment}-${local.region_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"
  
  tags = local.common_tags
}

# Network Security Group
resource "azurerm_network_security_group" "main" {
  name                = "nsg-hiscox-etl-${local.environment}-${local.region_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "10.0.0.0/8"
    destination_address_prefix = "*"
  }
  
  tags = local.common_tags
}
