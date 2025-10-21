# Configure the Azure Provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~>1.0"
    }
  }
  required_version = ">= 1.0"
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}

# Data source for current client configuration
data "azurerm_client_config" "current" {}

# Resource Group
resource "azurerm_resource_group" "hiscox_etl" {
  name     = var.resource_group_name
  location = var.location

  tags = var.tags
}

# Storage Account for Data Lake
resource "azurerm_storage_account" "data_lake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.hiscox_etl.name
  location                = azurerm_resource_group.hiscox_etl.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled          = true # Enable hierarchical namespace for Data Lake Gen2

  tags = var.tags
}

# Storage Containers
resource "azurerm_storage_container" "raw_data" {
  name                  = "raw-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed_data" {
  name                  = "processed-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "logs" {
  name                  = "logs"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

# Key Vault for secrets management
resource "azurerm_key_vault" "hiscox_kv" {
  name                = var.key_vault_name
  location            = azurerm_resource_group.hiscox_etl.location
  resource_group_name = azurerm_resource_group.hiscox_etl.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get", "List", "Create", "Delete", "Update", "Purge", "Recover"
    ]

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge", "Recover"
    ]

    storage_permissions = [
      "Get", "List", "Set", "Delete", "Purge", "Recover"
    ]
  }

  tags = var.tags
}

# Service Principal for Databricks
resource "azurerm_user_assigned_identity" "databricks_identity" {
  name                = "${var.project_name}-databricks-identity"
  location            = azurerm_resource_group.hiscox_etl.location
  resource_group_name = azurerm_resource_group.hiscox_etl.name

  tags = var.tags
}

# Role assignment for storage access
resource "azurerm_role_assignment" "databricks_storage" {
  scope                = azurerm_storage_account.data_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.databricks_identity.principal_id
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "hiscox_databricks" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.hiscox_etl.name
  location            = azurerm_resource_group.hiscox_etl.location
  sku                 = var.databricks_sku

  tags = var.tags
}

# Configure Databricks provider
provider "databricks" {
  host = azurerm_databricks_workspace.hiscox_databricks.workspace_url
}

# Databricks Cluster
resource "databricks_cluster" "etl_cluster" {
  cluster_name            = "${var.project_name}-etl-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id           = "Standard_DS3_v2"
  driver_node_type_id    = "Standard_DS3_v2"
  autotermination_minutes = 20
  num_workers            = 2

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
    "spark.sql.adaptive.enabled"             = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
  }

  library {
    pypi {
      package = "delta-spark==2.4.0"
    }
  }

  library {
    pypi {
      package = "azure-storage-blob==12.17.0"
    }
  }

  library {
    pypi {
      package = "great-expectations==0.17.23"
    }
  }

  depends_on = [azurerm_databricks_workspace.hiscox_databricks]
}

# Databricks Secret Scope
resource "databricks_secret_scope" "hiscox_secrets" {
  name = "hiscox-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.hiscox_kv.id
    dns_name    = azurerm_key_vault.hiscox_kv.vault_uri
  }

  depends_on = [azurerm_databricks_workspace.hiscox_databricks]
}

# Store storage account key in Key Vault
resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "storage-account-key"
  value        = azurerm_storage_account.data_lake.primary_access_key
  key_vault_id = azurerm_key_vault.hiscox_kv.id

  depends_on = [azurerm_key_vault.hiscox_kv]
}

# Store storage account name in Key Vault
resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "storage-account-name"
  value        = azurerm_storage_account.data_lake.name
  key_vault_id = azurerm_key_vault.hiscox_kv.id

  depends_on = [azurerm_key_vault.hiscox_kv]
}

# Container Registry for Docker images
resource "azurerm_container_registry" "hiscox_acr" {
  name                = var.container_registry_name
  resource_group_name = azurerm_resource_group.hiscox_etl.name
  location            = azurerm_resource_group.hiscox_etl.location
  sku                 = "Basic"
  admin_enabled       = true

  tags = var.tags
}

# Log Analytics Workspace for monitoring
resource "azurerm_log_analytics_workspace" "hiscox_logs" {
  name                = "${var.project_name}-logs"
  location            = azurerm_resource_group.hiscox_etl.location
  resource_group_name = azurerm_resource_group.hiscox_etl.name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = var.tags
}

# Application Insights for application monitoring
resource "azurerm_application_insights" "hiscox_insights" {
  name                = "${var.project_name}-insights"
  location            = azurerm_resource_group.hiscox_etl.location
  resource_group_name = azurerm_resource_group.hiscox_etl.name
  workspace_id        = azurerm_log_analytics_workspace.hiscox_logs.id
  application_type    = "other"

  tags = var.tags
}
