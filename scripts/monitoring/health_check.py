"""
Health check script for Hiscox ETL Pipeline
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import requests
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

@dataclass
class HealthCheckResult:
    """Health check result data class"""
    service: str
    status: str  # "healthy", "unhealthy", "degraded"
    response_time_ms: float
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None

class HealthChecker:
    """Health checker for ETL pipeline components"""
    
    def __init__(self, environment: str):
        self.environment = environment
        self.logger = structlog.get_logger(__name__)
        self.credential = DefaultAzureCredential()
        
        # Environment-specific configuration
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load environment-specific configuration"""
        
        base_config = {
            "timeout_seconds": 30,
            "retry_attempts": 3,
            "retry_delay_seconds": 5
        }
        
        env_configs = {
            "dev": {
                "storage_account": "sthiscoxetldev001am",
                "key_vault": "kv-hiscox-etl-dev-001am",
                "databricks_host": os.getenv("DATABRICKS_HOST_DEV", ""),
                "resource_group": "rg-hiscox-etl-dev"
            },
            "staging": {
                "storage_account": "sthiscoxetlstaging001am",
                "key_vault": "kv-hiscox-etl-staging-001am",
                "databricks_host": os.getenv("DATABRICKS_HOST_STAGING", ""),
                "resource_group": "rg-hiscox-etl-staging"
            },
            "production": {
                "storage_account": "sthiscoxetlprod001am",
                "key_vault": "kv-hiscox-etl-prod-001am",
                "databricks_host": os.getenv("DATABRICKS_HOST_PROD", ""),
                "resource_group": "rg-hiscox-etl-prod-001"
            }
        }
        
        config = {**base_config, **env_configs.get(self.environment, {})}
        return config
    
    def check_azure_storage(self) -> HealthCheckResult:
        """Check Azure Storage health"""
        
        start_time = time.time()
        
        try:
            storage_account = self.config["storage_account"]
            account_url = f"https://{storage_account}.blob.core.windows.net"
            
            blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential
            )
            
            # List containers to test connectivity
            containers = list(blob_service_client.list_containers())
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="azure_storage",
                status="healthy",
                response_time_ms=response_time,
                message=f"Successfully connected to storage account {storage_account}",
                details={
                    "storage_account": storage_account,
                    "containers_count": len(containers),
                    "containers": [c.name for c in containers]
                },
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="azure_storage",
                status="unhealthy",
                response_time_ms=response_time,
                message=f"Failed to connect to Azure Storage: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.utcnow()
            )
    
    def check_key_vault(self) -> HealthCheckResult:
        """Check Azure Key Vault health"""
        
        start_time = time.time()
        
        try:
            key_vault_name = self.config["key_vault"]
            vault_url = f"https://{key_vault_name}.vault.azure.net/"
            
            secret_client = SecretClient(
                vault_url=vault_url,
                credential=self.credential
            )
            
            # List secrets to test connectivity
            secrets = list(secret_client.list_properties_of_secrets())
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="key_vault",
                status="healthy",
                response_time_ms=response_time,
                message=f"Successfully connected to Key Vault {key_vault_name}",
                details={
                    "key_vault": key_vault_name,
                    "secrets_count": len(secrets)
                },
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="key_vault",
                status="unhealthy",
                response_time_ms=response_time,
                message=f"Failed to connect to Key Vault: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.utcnow()
            )
    
    def check_databricks(self) -> HealthCheckResult:
        """Check Databricks workspace health"""
        
        start_time = time.time()
        
        try:
            databricks_host = self.config["databricks_host"]
            if not databricks_host:
                return HealthCheckResult(
                    service="databricks",
                    status="unhealthy",
                    response_time_ms=0,
                    message="Databricks host not configured",
                    timestamp=datetime.utcnow()
                )
            
            # Get Databricks token from environment or Key Vault
            token = os.getenv(f"DATABRICKS_TOKEN_{self.environment.upper()}")
            if not token:
                # Try to get from Key Vault
                try:
                    key_vault_name = self.config["key_vault"]
                    vault_url = f"https://{key_vault_name}.vault.azure.net/"
                    secret_client = SecretClient(vault_url=vault_url, credential=self.credential)
                    token = secret_client.get_secret("databricks-token").value
                except:
                    pass
            
            if not token:
                return HealthCheckResult(
                    service="databricks",
                    status="unhealthy",
                    response_time_ms=0,
                    message="Databricks token not available",
                    timestamp=datetime.utcnow()
                )
            
            # Test Databricks API
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(
                f"{databricks_host}/api/2.0/clusters/list",
                headers=headers,
                timeout=self.config["timeout_seconds"]
            )
            
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                clusters = response.json().get("clusters", [])
                
                return HealthCheckResult(
                    service="databricks",
                    status="healthy",
                    response_time_ms=response_time,
                    message=f"Successfully connected to Databricks workspace",
                    details={
                        "workspace_url": databricks_host,
                        "clusters_count": len(clusters),
                        "active_clusters": len([c for c in clusters if c.get("state") == "RUNNING"])
                    },
                    timestamp=datetime.utcnow()
                )
            else:
                return HealthCheckResult(
                    service="databricks",
                    status="unhealthy",
                    response_time_ms=response_time,
                    message=f"Databricks API returned status {response.status_code}",
                    details={"status_code": response.status_code, "response": response.text},
                    timestamp=datetime.utcnow()
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="databricks",
                status="unhealthy",
                response_time_ms=response_time,
                message=f"Failed to connect to Databricks: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.utcnow()
            )
    
    def check_container_registry(self) -> HealthCheckResult:
        """Check Azure Container Registry health"""
        
        start_time = time.time()
        
        try:
            # Construct ACR name based on environment
            acr_name = f"acrhiscoxetl{self.environment}001am"
            if self.environment == "production":
                acr_name = "acrhiscoxetlprod001am"
            
            # Test ACR connectivity using Azure CLI (if available)
            import subprocess
            
            result = subprocess.run(
                ["az", "acr", "repository", "list", "--name", acr_name],
                capture_output=True,
                text=True,
                timeout=self.config["timeout_seconds"]
            )
            
            response_time = (time.time() - start_time) * 1000
            
            if result.returncode == 0:
                repositories = json.loads(result.stdout) if result.stdout else []
                
                return HealthCheckResult(
                    service="container_registry",
                    status="healthy",
                    response_time_ms=response_time,
                    message=f"Successfully connected to ACR {acr_name}",
                    details={
                        "registry_name": acr_name,
                        "repositories_count": len(repositories),
                        "repositories": repositories
                    },
                    timestamp=datetime.utcnow()
                )
            else:
                return HealthCheckResult(
                    service="container_registry",
                    status="unhealthy",
                    response_time_ms=response_time,
                    message=f"Failed to connect to ACR: {result.stderr}",
                    details={"error": result.stderr},
                    timestamp=datetime.utcnow()
                )
                
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="container_registry",
                status="unhealthy",
                response_time_ms=response_time,
                message=f"Failed to check Container Registry: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.utcnow()
            )
    
    def check_data_freshness(self) -> HealthCheckResult:
        """Check data freshness in storage"""
        
        start_time = time.time()
        
        try:
            storage_account = self.config["storage_account"]
            account_url = f"https://{storage_account}.blob.core.windows.net"
            
            blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential
            )
            
            # Check processed data container
            container_client = blob_service_client.get_container_client("processed-data")
            
            # Get latest blob
            blobs = list(container_client.list_blobs(name_starts_with="insurance/"))
            if not blobs:
                return HealthCheckResult(
                    service="data_freshness",
                    status="degraded",
                    response_time_ms=(time.time() - start_time) * 1000,
                    message="No processed data found",
                    timestamp=datetime.utcnow()
                )
            
            # Find most recent blob
            latest_blob = max(blobs, key=lambda b: b.last_modified)
            hours_old = (datetime.utcnow().replace(tzinfo=latest_blob.last_modified.tzinfo) - latest_blob.last_modified).total_seconds() / 3600
            
            response_time = (time.time() - start_time) * 1000
            
            # Data is considered stale if older than 24 hours
            if hours_old > 24:
                status = "degraded"
                message = f"Data is {hours_old:.1f} hours old (stale)"
            elif hours_old > 12:
                status = "degraded"
                message = f"Data is {hours_old:.1f} hours old (aging)"
            else:
                status = "healthy"
                message = f"Data is {hours_old:.1f} hours old (fresh)"
            
            return HealthCheckResult(
                service="data_freshness",
                status=status,
                response_time_ms=response_time,
                message=message,
                details={
                    "latest_blob": latest_blob.name,
                    "last_modified": latest_blob.last_modified.isoformat(),
                    "hours_old": hours_old,
                    "total_blobs": len(blobs)
                },
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                service="data_freshness",
                status="unhealthy",
                response_time_ms=response_time,
                message=f"Failed to check data freshness: {str(e)}",
                details={"error": str(e)},
                timestamp=datetime.utcnow()
            )
    
    def run_all_checks(self) -> List[HealthCheckResult]:
        """Run all health checks"""
        
        self.logger.info("Starting health checks", environment=self.environment)
        
        checks = [
            self.check_azure_storage,
            self.check_key_vault,
            self.check_databricks,
            self.check_container_registry,
            self.check_data_freshness
        ]
        
        results = []
        
        for check in checks:
            try:
                result = check()
                results.append(result)
                
                self.logger.info(
                    "Health check completed",
                    service=result.service,
                    status=result.status,
                    response_time_ms=result.response_time_ms,
                    message=result.message
                )
                
            except Exception as e:
                self.logger.error(
                    "Health check failed",
                    service=check.__name__,
                    error=str(e)
                )
                
                results.append(HealthCheckResult(
                    service=check.__name__,
                    status="unhealthy",
                    response_time_ms=0,
                    message=f"Health check failed: {str(e)}",
                    timestamp=datetime.utcnow()
                ))
        
        return results
    
    def generate_report(self, results: List[HealthCheckResult]) -> Dict[str, Any]:
        """Generate health check report"""
        
        overall_status = "healthy"
        if any(r.status == "unhealthy" for r in results):
            overall_status = "unhealthy"
        elif any(r.status == "degraded" for r in results):
            overall_status = "degraded"
        
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "environment": self.environment,
            "overall_status": overall_status,
            "total_checks": len(results),
            "healthy_checks": len([r for r in results if r.status == "healthy"]),
            "degraded_checks": len([r for r in results if r.status == "degraded"]),
            "unhealthy_checks": len([r for r in results if r.status == "unhealthy"]),
            "checks": []
        }
        
        for result in results:
            check_data = {
                "service": result.service,
                "status": result.status,
                "response_time_ms": result.response_time_ms,
                "message": result.message,
                "timestamp": result.timestamp.isoformat() if result.timestamp else None
            }
            
            if result.details:
                check_data["details"] = result.details
            
            report["checks"].append(check_data)
        
        return report

def main():
    """Main function"""
    
    parser = argparse.ArgumentParser(description="Health check for Hiscox ETL Pipeline")
    parser.add_argument("--environment", "-e", required=True, 
                       choices=["dev", "staging", "production"],
                       help="Environment to check")
    parser.add_argument("--output", "-o", choices=["json", "text"], default="text",
                       help="Output format")
    parser.add_argument("--output-file", "-f", help="Output file path")
    
    args = parser.parse_args()
    
    # Initialize health checker
    checker = HealthChecker(args.environment)
    
    # Run health checks
    results = checker.run_all_checks()
    
    # Generate report
    report = checker.generate_report(results)
    
    # Output results
    if args.output == "json":
        output = json.dumps(report, indent=2)
    else:
        # Text format
        lines = [
            f"Health Check Report - {args.environment.upper()}",
            f"Timestamp: {report['timestamp']}",
            f"Overall Status: {report['overall_status'].upper()}",
            f"Total Checks: {report['total_checks']}",
            f"Healthy: {report['healthy_checks']}, Degraded: {report['degraded_checks']}, Unhealthy: {report['unhealthy_checks']}",
            "",
            "Individual Checks:"
        ]
        
        for check in report['checks']:
            status_emoji = {"healthy": "✅", "degraded": "⚠️", "unhealthy": "❌"}
            lines.append(f"  {status_emoji.get(check['status'], '❓')} {check['service']}: {check['status'].upper()}")
            lines.append(f"     Message: {check['message']}")
            lines.append(f"     Response Time: {check['response_time_ms']:.1f}ms")
            lines.append("")
        
        output = "\n".join(lines)
    
    # Write to file or stdout
    if args.output_file:
        with open(args.output_file, 'w') as f:
            f.write(output)
        print(f"Health check report written to {args.output_file}")
    else:
        print(output)
    
    # Exit with appropriate code
    if report['overall_status'] == "unhealthy":
        sys.exit(1)
    elif report['overall_status'] == "degraded":
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
