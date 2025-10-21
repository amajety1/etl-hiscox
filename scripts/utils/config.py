#!/usr/bin/env python3
"""
Configuration management for Hiscox ETL Pipeline
"""

import os
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class Config:
    """Configuration class for ETL pipeline"""
    
    # Environment
    environment: str = os.getenv("ENVIRONMENT", "dev")
    
    # Azure Configuration
    azure_subscription_id: Optional[str] = os.getenv("AZURE_SUBSCRIPTION_ID")
    azure_tenant_id: Optional[str] = os.getenv("AZURE_TENANT_ID")
    azure_client_id: Optional[str] = os.getenv("AZURE_CLIENT_ID")
    azure_client_secret: Optional[str] = os.getenv("AZURE_CLIENT_SECRET")
    
    # Storage Configuration
    storage_account_name: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "sthiscoxetldev001")
    storage_account_key: Optional[str] = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    storage_connection_string: Optional[str] = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    # Key Vault Configuration
    key_vault_url: str = os.getenv("KEY_VAULT_URL", "https://kv-hiscox-etl-dev-001.vault.azure.net/")
    
    # Databricks Configuration
    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_token: Optional[str] = os.getenv("DATABRICKS_TOKEN")
    databricks_http_path: str = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/")
    databricks_cluster_id: Optional[str] = os.getenv("DATABRICKS_CLUSTER_ID")
    
    # Database Configuration
    database_name: str = f"hiscox_etl_{environment}"
    
    # Container Registry
    container_registry_url: str = os.getenv("CONTAINER_REGISTRY_URL", "acrhiscoxetldev001.azurecr.io")
    
    # Logging Configuration
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_format: str = os.getenv("LOG_FORMAT", "json")
    
    # Data Quality Thresholds
    max_null_percentage: float = float(os.getenv("MAX_NULL_PERCENTAGE", "0.05"))
    min_row_count: int = int(os.getenv("MIN_ROW_COUNT", "100"))
    max_duplicate_percentage: float = float(os.getenv("MAX_DUPLICATE_PERCENTAGE", "0.01"))
    
    # Pipeline Configuration
    batch_size: int = int(os.getenv("BATCH_SIZE", "10000"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY", "60"))  # seconds
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.databricks_host and self.environment != "local":
            raise ValueError("DATABRICKS_HOST is required for non-local environments")
        
        if not self.storage_account_name:
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME is required")
    
    @property
    def is_local_env(self) -> bool:
        """Check if running in local environment"""
        return self.environment.lower() in ["local", "dev", "development"]
    
    @property
    def is_production_env(self) -> bool:
        """Check if running in production environment"""
        return self.environment.lower() in ["prod", "production"]
    
    def get_storage_url(self, container: str) -> str:
        """Get storage URL for a specific container"""
        return f"abfss://{container}@{self.storage_account_name}.dfs.core.windows.net/"
    
    def get_table_name(self, layer: str, table: str) -> str:
        """Get fully qualified table name"""
        return f"{self.database_name}_{layer}.{table}"
