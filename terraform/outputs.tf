# Resource Group Outputs
output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.hiscox_etl.name
}

output "resource_group_location" {
  description = "Location of the created resource group"
  value       = azurerm_resource_group.hiscox_etl.location
}

# Storage Account Outputs
output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.data_lake.name
}

output "storage_account_primary_endpoint" {
  description = "Primary endpoint of the storage account"
  value       = azurerm_storage_account.data_lake.primary_dfs_endpoint
}

output "storage_account_id" {
  description = "ID of the storage account"
  value       = azurerm_storage_account.data_lake.id
}

# Storage Container Outputs
output "raw_data_container_name" {
  description = "Name of the raw data container"
  value       = azurerm_storage_container.raw_data.name
}

output "processed_data_container_name" {
  description = "Name of the processed data container"
  value       = azurerm_storage_container.processed_data.name
}

output "logs_container_name" {
  description = "Name of the logs container"
  value       = azurerm_storage_container.logs.name
}

# Key Vault Outputs
output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.hiscox_kv.name
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.hiscox_kv.vault_uri
}

output "key_vault_id" {
  description = "ID of the Key Vault"
  value       = azurerm_key_vault.hiscox_kv.id
}

# Databricks Outputs
output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.hiscox_databricks.name
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = "https://${azurerm_databricks_workspace.hiscox_databricks.workspace_url}"
}

output "databricks_workspace_id" {
  description = "ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.hiscox_databricks.workspace_id
}

# Container Registry Outputs
output "container_registry_name" {
  description = "Name of the Container Registry"
  value       = azurerm_container_registry.hiscox_acr.name
}

output "container_registry_login_server" {
  description = "Login server of the Container Registry"
  value       = azurerm_container_registry.hiscox_acr.login_server
}

output "container_registry_admin_username" {
  description = "Admin username for Container Registry"
  value       = azurerm_container_registry.hiscox_acr.admin_username
  sensitive   = true
}

output "container_registry_admin_password" {
  description = "Admin password for Container Registry"
  value       = azurerm_container_registry.hiscox_acr.admin_password
  sensitive   = true
}

# Managed Identity Outputs
output "databricks_identity_principal_id" {
  description = "Principal ID of the Databricks managed identity"
  value       = azurerm_user_assigned_identity.databricks_identity.principal_id
}

output "databricks_identity_client_id" {
  description = "Client ID of the Databricks managed identity"
  value       = azurerm_user_assigned_identity.databricks_identity.client_id
}

# Monitoring Outputs
output "log_analytics_workspace_id" {
  description = "ID of the Log Analytics workspace"
  value       = azurerm_log_analytics_workspace.hiscox_logs.id
}

output "application_insights_instrumentation_key" {
  description = "Instrumentation key for Application Insights"
  value       = azurerm_application_insights.hiscox_insights.instrumentation_key
  sensitive   = true
}

output "application_insights_connection_string" {
  description = "Connection string for Application Insights"
  value       = azurerm_application_insights.hiscox_insights.connection_string
  sensitive   = true
}

# Environment Configuration for Applications
output "environment_config" {
  description = "Environment configuration for applications"
  value = {
    storage_account_name    = azurerm_storage_account.data_lake.name
    key_vault_name         = azurerm_key_vault.hiscox_kv.name
    databricks_workspace_url = azurerm_databricks_workspace.hiscox_databricks.workspace_url
    container_registry_url = azurerm_container_registry.hiscox_acr.login_server
    resource_group_name    = azurerm_resource_group.hiscox_etl.name
    location              = azurerm_resource_group.hiscox_etl.location
  }
}

# Connection Strings (for local development)
output "storage_connection_string" {
  description = "Connection string for storage account (for local development only)"
  value       = azurerm_storage_account.data_lake.primary_connection_string
  sensitive   = true
}
