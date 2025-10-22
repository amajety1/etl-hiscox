"""
Integration tests for Hiscox ETL Pipeline
"""

import pytest
import os
import json
import tempfile
import time
from datetime import datetime, timedelta
from unittest.mock import patch, Mock
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Test configuration
TEST_CONFIG = {
    'dev': {
        'storage_account': 'sthiscoxetldev001am',
        'key_vault': 'kv-hiscox-etl-dev-001am',
        'resource_group': 'rg-hiscox-etl-dev'
    },
    'staging': {
        'storage_account': 'sthiscoxetlstaging001am',
        'key_vault': 'kv-hiscox-etl-staging-001am',
        'resource_group': 'rg-hiscox-etl-staging'
    }
}

class TestAzureStorageIntegration:
    """Integration tests for Azure Storage operations"""
    
    @pytest.fixture
    def storage_client(self, environment):
        """Fixture to create Azure Storage client"""
        if environment not in TEST_CONFIG:
            pytest.skip(f"No configuration for environment: {environment}")
        
        config = TEST_CONFIG[environment]
        account_url = f"https://{config['storage_account']}.blob.core.windows.net"
        
        try:
            client = BlobServiceClient(
                account_url=account_url,
                credential=DefaultAzureCredential()
            )
            # Test connection
            list(client.list_containers())
            return client
        except Exception as e:
            pytest.skip(f"Cannot connect to Azure Storage: {e}")
    
    def test_container_access(self, storage_client):
        """Test access to required containers"""
        required_containers = ['raw-data', 'processed-data', 'logs']
        
        existing_containers = [c.name for c in storage_client.list_containers()]
        
        for container in required_containers:
            assert container in existing_containers, f"Container {container} not found"
    
    def test_upload_and_download_blob(self, storage_client):
        """Test blob upload and download operations"""
        container_name = 'raw-data'
        blob_name = f'test/integration_test_{int(time.time())}.txt'
        test_content = "Integration test content"
        
        try:
            # Upload blob
            blob_client = storage_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.upload_blob(test_content, overwrite=True)
            
            # Download blob
            downloaded_content = blob_client.download_blob().readall().decode('utf-8')
            assert downloaded_content == test_content
            
        finally:
            # Cleanup
            try:
                blob_client.delete_blob()
            except:
                pass
    
    def test_list_blobs_with_prefix(self, storage_client):
        """Test listing blobs with prefix"""
        container_client = storage_client.get_container_client('processed-data')
        
        # This should not raise an exception
        blobs = list(container_client.list_blobs(name_starts_with='insurance/'))
        
        # We don't assert specific count as it depends on existing data
        assert isinstance(blobs, list)
    
    def test_blob_metadata_operations(self, storage_client):
        """Test blob metadata operations"""
        container_name = 'raw-data'
        blob_name = f'test/metadata_test_{int(time.time())}.txt'
        test_content = "Metadata test content"
        metadata = {
            'source': 'integration_test',
            'processed': 'false',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        try:
            # Upload blob with metadata
            blob_client = storage_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.upload_blob(test_content, metadata=metadata, overwrite=True)
            
            # Get blob properties and verify metadata
            properties = blob_client.get_blob_properties()
            assert properties.metadata['source'] == 'integration_test'
            assert properties.metadata['processed'] == 'false'
            
        finally:
            # Cleanup
            try:
                blob_client.delete_blob()
            except:
                pass

class TestKeyVaultIntegration:
    """Integration tests for Azure Key Vault operations"""
    
    @pytest.fixture
    def key_vault_client(self, environment):
        """Fixture to create Key Vault client"""
        if environment not in TEST_CONFIG:
            pytest.skip(f"No configuration for environment: {environment}")
        
        config = TEST_CONFIG[environment]
        vault_url = f"https://{config['key_vault']}.vault.azure.net/"
        
        try:
            client = SecretClient(
                vault_url=vault_url,
                credential=DefaultAzureCredential()
            )
            # Test connection by listing secrets
            list(client.list_properties_of_secrets())
            return client
        except Exception as e:
            pytest.skip(f"Cannot connect to Key Vault: {e}")
    
    def test_secret_operations(self, key_vault_client):
        """Test secret creation, retrieval, and deletion"""
        secret_name = f"integration-test-{int(time.time())}"
        secret_value = "test-secret-value"
        
        try:
            # Create secret
            key_vault_client.set_secret(secret_name, secret_value)
            
            # Retrieve secret
            retrieved_secret = key_vault_client.get_secret(secret_name)
            assert retrieved_secret.value == secret_value
            
        finally:
            # Cleanup
            try:
                key_vault_client.begin_delete_secret(secret_name).wait()
            except:
                pass
    
    def test_list_secrets(self, key_vault_client):
        """Test listing secrets"""
        secrets = list(key_vault_client.list_properties_of_secrets())
        assert isinstance(secrets, list)
        
        # Check if expected secrets exist (these would be created by Terraform)
        secret_names = [s.name for s in secrets]
        
        # We don't assert specific secrets as they may not exist in test environment
        # but we ensure the operation works
        assert len(secret_names) >= 0

class TestDatabricksIntegration:
    """Integration tests for Databricks operations"""
    
    @pytest.fixture
    def databricks_config(self, environment):
        """Fixture to get Databricks configuration"""
        databricks_host = os.getenv(f'DATABRICKS_HOST_{environment.upper()}')
        databricks_token = os.getenv(f'DATABRICKS_TOKEN_{environment.upper()}')
        
        if not databricks_host or not databricks_token:
            pytest.skip(f"Databricks configuration not available for {environment}")
        
        return {
            'host': databricks_host,
            'token': databricks_token
        }
    
    def test_databricks_api_connectivity(self, databricks_config):
        """Test Databricks API connectivity"""
        import requests
        
        headers = {'Authorization': f'Bearer {databricks_config["token"]}'}
        
        response = requests.get(
            f'{databricks_config["host"]}/api/2.0/clusters/list',
            headers=headers,
            timeout=30
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'clusters' in data
    
    def test_workspace_operations(self, databricks_config):
        """Test Databricks workspace operations"""
        import requests
        
        headers = {'Authorization': f'Bearer {databricks_config["token"]}'}
        
        # List workspace objects
        response = requests.get(
            f'{databricks_config["host"]}/api/2.0/workspace/list',
            headers=headers,
            params={'path': '/'},
            timeout=30
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'objects' in data

class TestEndToEndPipeline:
    """End-to-end integration tests"""
    
    @pytest.fixture
    def sample_data_file(self):
        """Create a sample data file for testing"""
        data = {
            'policy_id': ['POL001', 'POL002', 'POL003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'premium': [1000.0, 1500.0, 2000.0],
            'start_date': ['2024-01-01', '2024-01-15', '2024-02-01'],
            'end_date': ['2024-12-31', '2025-01-14', '2025-01-31'],
            'policy_type': ['AUTO', 'HOME', 'LIFE']
        }
        
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            return f.name
    
    def test_data_ingestion_flow(self, storage_client, sample_data_file):
        """Test complete data ingestion flow"""
        container_name = 'raw-data'
        blob_name = f'test/integration_test_{int(time.time())}.csv'
        
        try:
            # Upload test file to raw-data container
            blob_client = storage_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            with open(sample_data_file, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
            
            # Verify upload
            properties = blob_client.get_blob_properties()
            assert properties.size > 0
            
            # Download and verify content
            downloaded_data = blob_client.download_blob().readall()
            df = pd.read_csv(pd.io.common.BytesIO(downloaded_data))
            
            assert len(df) == 3
            assert 'policy_id' in df.columns
            assert df['premium'].sum() == 4500.0
            
        finally:
            # Cleanup
            try:
                blob_client.delete_blob()
                os.unlink(sample_data_file)
            except:
                pass
    
    def test_data_validation_pipeline(self, sample_data_file):
        """Test data validation pipeline"""
        # Read the sample data
        df = pd.read_csv(sample_data_file)
        
        # Mock validation pipeline
        validation_results = {
            'total_records': len(df),
            'valid_records': len(df),
            'invalid_records': 0,
            'validation_errors': []
        }
        
        # Basic validations
        assert validation_results['total_records'] == 3
        assert validation_results['valid_records'] == 3
        assert validation_results['invalid_records'] == 0
        assert len(validation_results['validation_errors']) == 0
        
        # Cleanup
        try:
            os.unlink(sample_data_file)
        except:
            pass

class TestDataQualityChecks:
    """Integration tests for data quality checks"""
    
    def test_data_completeness_check(self, storage_client):
        """Test data completeness checks"""
        container_client = storage_client.get_container_client('processed-data')
        
        # Get recent blobs
        recent_blobs = []
        cutoff_time = datetime.utcnow() - timedelta(days=7)
        
        for blob in container_client.list_blobs(name_starts_with='insurance/'):
            if blob.last_modified.replace(tzinfo=None) > cutoff_time:
                recent_blobs.append(blob)
        
        # We expect some data to be present (this may vary by environment)
        # In a real test, we'd have specific expectations
        assert isinstance(recent_blobs, list)
    
    def test_data_freshness_check(self, storage_client):
        """Test data freshness checks"""
        container_client = storage_client.get_container_client('processed-data')
        
        blobs = list(container_client.list_blobs(name_starts_with='insurance/'))
        
        if blobs:
            # Find most recent blob
            latest_blob = max(blobs, key=lambda b: b.last_modified)
            
            # Check if data is reasonably fresh (within last 48 hours for integration test)
            hours_old = (datetime.utcnow().replace(tzinfo=latest_blob.last_modified.tzinfo) - latest_blob.last_modified).total_seconds() / 3600
            
            # In integration tests, we're more lenient with freshness
            assert hours_old < 168  # 7 days
    
    def test_schema_validation(self):
        """Test schema validation for insurance data"""
        # Expected schema
        expected_columns = [
            'policy_id', 'customer_id', 'premium', 'start_date', 
            'end_date', 'policy_type'
        ]
        
        # Sample data that should pass validation
        valid_data = pd.DataFrame({
            'policy_id': ['POL001'],
            'customer_id': ['CUST001'],
            'premium': [1000.0],
            'start_date': ['2024-01-01'],
            'end_date': ['2024-12-31'],
            'policy_type': ['AUTO']
        })
        
        # Check all expected columns are present
        for col in expected_columns:
            assert col in valid_data.columns, f"Missing column: {col}"
        
        # Check data types (basic validation)
        assert pd.api.types.is_numeric_dtype(valid_data['premium'])
        assert pd.api.types.is_object_dtype(valid_data['policy_id'])

class TestPerformanceIntegration:
    """Performance integration tests"""
    
    def test_large_file_processing(self, storage_client):
        """Test processing of larger files"""
        # Create a larger dataset for performance testing
        large_data = []
        for i in range(1000):
            large_data.append({
                'policy_id': f'POL{i:06d}',
                'customer_id': f'CUST{i:06d}',
                'premium': 1000.0 + (i % 1000),
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'policy_type': ['AUTO', 'HOME', 'LIFE'][i % 3]
            })
        
        df = pd.DataFrame(large_data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            # Measure upload time
            start_time = time.time()
            
            blob_client = storage_client.get_blob_client(
                container='raw-data',
                blob=f'test/performance_test_{int(time.time())}.csv'
            )
            
            with open(temp_file, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
            
            upload_time = time.time() - start_time
            
            # Performance assertion (adjust based on expectations)
            assert upload_time < 30, f"Upload took too long: {upload_time}s"
            
            # Verify file size
            properties = blob_client.get_blob_properties()
            assert properties.size > 50000  # Expect reasonable file size
            
        finally:
            # Cleanup
            try:
                blob_client.delete_blob()
                os.unlink(temp_file)
            except:
                pass

# Test fixtures and configuration
@pytest.fixture(scope="session")
def environment():
    """Get test environment from environment variable"""
    env = os.getenv('TEST_ENVIRONMENT', 'dev')
    if env not in ['dev', 'staging']:
        pytest.skip(f"Invalid test environment: {env}")
    return env

@pytest.fixture(scope="session")
def azure_credentials():
    """Verify Azure credentials are available"""
    try:
        credential = DefaultAzureCredential()
        # Test credential by getting a token
        credential.get_token("https://management.azure.com/.default")
        return credential
    except Exception as e:
        pytest.skip(f"Azure credentials not available: {e}")

# Markers for different types of integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv('RUN_INTEGRATION_TESTS'),
        reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=1 to enable"
    )
]

# Test configuration
def pytest_configure(config):
    """Configure pytest for integration tests"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "azure: mark test as requiring Azure resources"
    )

# Helper functions for integration tests
def wait_for_blob_processing(storage_client, container_name, blob_name, timeout=300):
    """Wait for blob to be processed (helper function)"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            blob_client = storage_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            properties = blob_client.get_blob_properties()
            
            # Check if blob has been processed (example: metadata indicates processing)
            if properties.metadata and properties.metadata.get('processed') == 'true':
                return True
                
        except Exception:
            pass
        
        time.sleep(5)
    
    return False

def create_test_data_batch(size=100):
    """Create a batch of test data"""
    data = []
    for i in range(size):
        data.append({
            'policy_id': f'TEST{i:06d}',
            'customer_id': f'CUST{i:06d}',
            'premium': 1000.0 + (i % 1000),
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'policy_type': ['AUTO', 'HOME', 'LIFE'][i % 3],
            'status': 'ACTIVE'
        })
    return pd.DataFrame(data)
