#!/usr/bin/env python3
"""
Hiscox ETL Pipeline - Data Ingestion Script
Ingests raw insurance data from Azure Blob Storage to Databricks Delta Lake
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config import Config
from logger import setup_logger

class DataIngestion:
    """Handles data ingestion from Azure Blob Storage to Databricks Delta Lake"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger(__name__)
        self.spark = self._initialize_spark()
        self.blob_client = self._initialize_blob_client()
        
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with Delta Lake configuration"""
        try:
            spark = SparkSession.builder \
                .appName("HiscoxETL-Ingestion") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
                .getOrCreate()
            
            self.logger.info("Spark session initialized successfully")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise
    
    def _initialize_blob_client(self) -> BlobServiceClient:
        """Initialize Azure Blob Storage client"""
        try:
            credential = DefaultAzureCredential()
            blob_client = BlobServiceClient(
                account_url=f"https://{self.config.storage_account_name}.blob.core.windows.net",
                credential=credential
            )
            self.logger.info("Blob storage client initialized successfully")
            return blob_client
            
        except Exception as e:
            self.logger.error(f"Failed to initialize blob client: {str(e)}")
            raise
    
    def ingest_claims_data(self, source_path: str, target_table: str) -> bool:
        """Ingest claims data from blob storage to Delta Lake"""
        try:
            self.logger.info(f"Starting claims data ingestion from {source_path}")
            
            # Read data from blob storage
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source_path)
            
            # Add metadata columns
            df_with_metadata = df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(source_path)) \
                .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            
            # Data quality checks
            initial_count = df_with_metadata.count()
            self.logger.info(f"Initial record count: {initial_count}")
            
            # Remove duplicates based on claim_id
            df_deduplicated = df_with_metadata.dropDuplicates(["claim_id"])
            final_count = df_deduplicated.count()
            
            if initial_count != final_count:
                self.logger.warning(f"Removed {initial_count - final_count} duplicate records")
            
            # Write to Delta Lake
            df_deduplicated.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            
            self.logger.info(f"Successfully ingested {final_count} claims records to {target_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to ingest claims data: {str(e)}")
            return False
    
    def ingest_policies_data(self, source_path: str, target_table: str) -> bool:
        """Ingest policies data from blob storage to Delta Lake"""
        try:
            self.logger.info(f"Starting policies data ingestion from {source_path}")
            
            # Read data from blob storage
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source_path)
            
            # Add metadata columns
            df_with_metadata = df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(source_path)) \
                .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            
            # Data quality checks
            initial_count = df_with_metadata.count()
            self.logger.info(f"Initial record count: {initial_count}")
            
            # Remove duplicates based on policy_id
            df_deduplicated = df_with_metadata.dropDuplicates(["policy_id"])
            final_count = df_deduplicated.count()
            
            if initial_count != final_count:
                self.logger.warning(f"Removed {initial_count - final_count} duplicate records")
            
            # Write to Delta Lake
            df_deduplicated.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            
            self.logger.info(f"Successfully ingested {final_count} policies records to {target_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to ingest policies data: {str(e)}")
            return False
    
    def run_ingestion_pipeline(self) -> Dict[str, bool]:
        """Run the complete data ingestion pipeline"""
        results = {}
        
        try:
            self.logger.info("Starting Hiscox ETL data ingestion pipeline")
            
            # Ingest claims data
            claims_result = self.ingest_claims_data(
                source_path=f"abfss://raw-data@{self.config.storage_account_name}.dfs.core.windows.net/claims/claims.csv",
                target_table=f"{self.config.database_name}_bronze.claims"
            )
            results['claims'] = claims_result
            
            # Ingest policies data
            policies_result = self.ingest_policies_data(
                source_path=f"abfss://raw-data@{self.config.storage_account_name}.dfs.core.windows.net/policies/policies.csv",
                target_table=f"{self.config.database_name}_bronze.policies"
            )
            results['policies'] = policies_result
            
            # Log overall results
            successful_ingestions = sum(results.values())
            total_ingestions = len(results)
            
            self.logger.info(f"Ingestion pipeline completed: {successful_ingestions}/{total_ingestions} successful")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Ingestion pipeline failed: {str(e)}")
            return results
        
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main execution function"""
    try:
        # Initialize configuration
        config = Config()
        
        # Run ingestion pipeline
        ingestion = DataIngestion(config)
        results = ingestion.run_ingestion_pipeline()
        
        # Exit with appropriate code
        if all(results.values()):
            print("✅ Data ingestion completed successfully")
            sys.exit(0)
        else:
            print("❌ Data ingestion completed with errors")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Fatal error in data ingestion: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
