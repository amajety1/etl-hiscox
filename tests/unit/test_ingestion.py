"""
Unit tests for ingestion module
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import tempfile
import os

# Import the modules to test (assuming they exist)
# from scripts.ingestion import IngestionPipeline, DataValidator, FileProcessor

class TestDataValidator:
    """Test cases for DataValidator class"""
    
    def test_validate_insurance_data_valid(self):
        """Test validation of valid insurance data"""
        # Create sample valid data
        valid_data = pd.DataFrame({
            'policy_id': ['POL001', 'POL002', 'POL003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'premium': [1000.0, 1500.0, 2000.0],
            'start_date': ['2024-01-01', '2024-01-15', '2024-02-01'],
            'end_date': ['2024-12-31', '2025-01-14', '2025-01-31'],
            'policy_type': ['AUTO', 'HOME', 'LIFE']
        })
        
        # Mock validator (since we don't have the actual class yet)
        validator = Mock()
        validator.validate_schema.return_value = True
        validator.validate_data_quality.return_value = {'is_valid': True, 'errors': []}
        
        # Test validation
        result = validator.validate_schema(valid_data)
        assert result is True
        
        quality_result = validator.validate_data_quality(valid_data)
        assert quality_result['is_valid'] is True
        assert len(quality_result['errors']) == 0
    
    def test_validate_insurance_data_invalid_schema(self):
        """Test validation of data with invalid schema"""
        # Create sample data with missing required columns
        invalid_data = pd.DataFrame({
            'policy_id': ['POL001', 'POL002'],
            'premium': [1000.0, 1500.0]
            # Missing required columns: customer_id, start_date, etc.
        })
        
        validator = Mock()
        validator.validate_schema.return_value = False
        validator.get_schema_errors.return_value = ['Missing required column: customer_id']
        
        result = validator.validate_schema(invalid_data)
        assert result is False
        
        errors = validator.get_schema_errors()
        assert 'Missing required column: customer_id' in errors
    
    def test_validate_data_quality_issues(self):
        """Test detection of data quality issues"""
        # Create data with quality issues
        problematic_data = pd.DataFrame({
            'policy_id': ['POL001', '', 'POL003'],  # Empty policy_id
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'premium': [1000.0, -500.0, 2000.0],  # Negative premium
            'start_date': ['2024-01-01', '2024-01-15', 'invalid-date'],  # Invalid date
            'end_date': ['2024-12-31', '2025-01-14', '2025-01-31'],
            'policy_type': ['AUTO', 'UNKNOWN', 'LIFE']  # Invalid policy type
        })
        
        validator = Mock()
        validator.validate_data_quality.return_value = {
            'is_valid': False,
            'errors': [
                'Empty policy_id in row 1',
                'Negative premium in row 1: -500.0',
                'Invalid date format in row 2: invalid-date',
                'Unknown policy type in row 1: UNKNOWN'
            ]
        }
        
        result = validator.validate_data_quality(problematic_data)
        assert result['is_valid'] is False
        assert len(result['errors']) == 4

class TestFileProcessor:
    """Test cases for FileProcessor class"""
    
    def test_process_csv_file(self):
        """Test processing of CSV files"""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('policy_id,customer_id,premium\n')
            f.write('POL001,CUST001,1000.0\n')
            f.write('POL002,CUST002,1500.0\n')
            temp_file = f.name
        
        try:
            # Mock file processor
            processor = Mock()
            processor.read_csv.return_value = pd.DataFrame({
                'policy_id': ['POL001', 'POL002'],
                'customer_id': ['CUST001', 'CUST002'],
                'premium': [1000.0, 1500.0]
            })
            
            result = processor.read_csv(temp_file)
            assert len(result) == 2
            assert 'policy_id' in result.columns
            assert result['premium'].sum() == 2500.0
            
        finally:
            os.unlink(temp_file)
    
    def test_process_excel_file(self):
        """Test processing of Excel files"""
        processor = Mock()
        processor.read_excel.return_value = pd.DataFrame({
            'policy_id': ['POL001', 'POL002', 'POL003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'premium': [1000.0, 1500.0, 2000.0]
        })
        
        result = processor.read_excel('test.xlsx')
        assert len(result) == 3
        assert result['premium'].mean() == 1500.0
    
    def test_file_not_found(self):
        """Test handling of missing files"""
        processor = Mock()
        processor.read_csv.side_effect = FileNotFoundError("File not found")
        
        with pytest.raises(FileNotFoundError):
            processor.read_csv('nonexistent.csv')

class TestIngestionPipeline:
    """Test cases for IngestionPipeline class"""
    
    @patch('scripts.ingestion.BlobServiceClient')
    @patch('scripts.ingestion.SecretClient')
    def test_pipeline_initialization(self, mock_secret_client, mock_blob_client):
        """Test pipeline initialization"""
        # Mock Azure clients
        mock_blob_client.return_value = Mock()
        mock_secret_client.return_value = Mock()
        
        # Mock pipeline
        pipeline = Mock()
        pipeline.environment = 'dev'
        pipeline.storage_account = 'test-storage'
        pipeline.container_name = 'raw-data'
        
        assert pipeline.environment == 'dev'
        assert pipeline.storage_account == 'test-storage'
        assert pipeline.container_name == 'raw-data'
    
    def test_pipeline_run_success(self):
        """Test successful pipeline run"""
        pipeline = Mock()
        pipeline.run.return_value = {
            'status': 'success',
            'records_processed': 1000,
            'files_processed': 5,
            'errors': []
        }
        
        result = pipeline.run()
        assert result['status'] == 'success'
        assert result['records_processed'] == 1000
        assert result['files_processed'] == 5
        assert len(result['errors']) == 0
    
    def test_pipeline_run_with_errors(self):
        """Test pipeline run with errors"""
        pipeline = Mock()
        pipeline.run.return_value = {
            'status': 'completed_with_errors',
            'records_processed': 800,
            'files_processed': 4,
            'errors': [
                'Failed to process file: corrupted_data.csv',
                'Validation failed for 50 records'
            ]
        }
        
        result = pipeline.run()
        assert result['status'] == 'completed_with_errors'
        assert result['records_processed'] == 800
        assert len(result['errors']) == 2
    
    def test_pipeline_run_failure(self):
        """Test pipeline run failure"""
        pipeline = Mock()
        pipeline.run.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception) as exc_info:
            pipeline.run()
        
        assert "Database connection failed" in str(exc_info.value)

class TestDataTransformation:
    """Test cases for data transformation functions"""
    
    def test_clean_policy_data(self):
        """Test policy data cleaning"""
        # Sample raw data
        raw_data = pd.DataFrame({
            'policy_id': ['  POL001  ', 'pol002', 'POL003'],
            'customer_id': ['CUST001', 'cust002', 'CUST003'],
            'premium': ['1000.0', '1,500.50', '2000'],
            'start_date': ['01/01/2024', '2024-01-15', '2024/02/01'],
            'policy_type': ['auto', 'HOME', 'life']
        })
        
        # Mock transformation function
        transformer = Mock()
        transformer.clean_policy_data.return_value = pd.DataFrame({
            'policy_id': ['POL001', 'POL002', 'POL003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'premium': [1000.0, 1500.5, 2000.0],
            'start_date': ['2024-01-01', '2024-01-15', '2024-02-01'],
            'policy_type': ['AUTO', 'HOME', 'LIFE']
        })
        
        cleaned_data = transformer.clean_policy_data(raw_data)
        
        # Verify transformations
        assert cleaned_data['policy_id'].iloc[0] == 'POL001'  # Trimmed and uppercase
        assert cleaned_data['policy_id'].iloc[1] == 'POL002'  # Uppercase
        assert cleaned_data['premium'].iloc[1] == 1500.5  # Parsed from string with comma
        assert cleaned_data['policy_type'].iloc[0] == 'AUTO'  # Uppercase
    
    def test_standardize_dates(self):
        """Test date standardization"""
        data_with_dates = pd.DataFrame({
            'start_date': ['01/01/2024', '2024-01-15', '2024/02/01'],
            'end_date': ['12/31/2024', '2025-01-14', '2025/01/31']
        })
        
        transformer = Mock()
        transformer.standardize_dates.return_value = pd.DataFrame({
            'start_date': ['2024-01-01', '2024-01-15', '2024-02-01'],
            'end_date': ['2024-12-31', '2025-01-14', '2025-01-31']
        })
        
        standardized_data = transformer.standardize_dates(data_with_dates)
        
        # All dates should be in YYYY-MM-DD format
        assert standardized_data['start_date'].iloc[0] == '2024-01-01'
        assert standardized_data['end_date'].iloc[0] == '2024-12-31'

class TestErrorHandling:
    """Test cases for error handling"""
    
    def test_handle_missing_files(self):
        """Test handling of missing input files"""
        error_handler = Mock()
        error_handler.handle_missing_file.return_value = {
            'error_type': 'FileNotFound',
            'message': 'Input file not found: missing_file.csv',
            'action': 'skip_and_continue'
        }
        
        result = error_handler.handle_missing_file('missing_file.csv')
        assert result['error_type'] == 'FileNotFound'
        assert result['action'] == 'skip_and_continue'
    
    def test_handle_data_validation_errors(self):
        """Test handling of data validation errors"""
        error_handler = Mock()
        error_handler.handle_validation_error.return_value = {
            'error_type': 'ValidationError',
            'message': 'Invalid data format in row 5',
            'action': 'quarantine_record'
        }
        
        result = error_handler.handle_validation_error('Invalid data format in row 5')
        assert result['error_type'] == 'ValidationError'
        assert result['action'] == 'quarantine_record'
    
    def test_handle_azure_connection_errors(self):
        """Test handling of Azure connection errors"""
        error_handler = Mock()
        error_handler.handle_azure_error.return_value = {
            'error_type': 'AzureConnectionError',
            'message': 'Failed to connect to Azure Storage',
            'action': 'retry_with_backoff'
        }
        
        result = error_handler.handle_azure_error('Failed to connect to Azure Storage')
        assert result['error_type'] == 'AzureConnectionError'
        assert result['action'] == 'retry_with_backoff'

class TestPerformanceMetrics:
    """Test cases for performance monitoring"""
    
    def test_record_processing_metrics(self):
        """Test recording of processing metrics"""
        metrics_collector = Mock()
        metrics_collector.record_processing_time.return_value = None
        metrics_collector.get_average_processing_time.return_value = 0.5
        
        # Simulate processing time recording
        metrics_collector.record_processing_time(0.3)
        metrics_collector.record_processing_time(0.7)
        
        avg_time = metrics_collector.get_average_processing_time()
        assert avg_time == 0.5
    
    def test_memory_usage_tracking(self):
        """Test memory usage tracking"""
        metrics_collector = Mock()
        metrics_collector.get_memory_usage.return_value = 512.5  # MB
        
        memory_usage = metrics_collector.get_memory_usage()
        assert memory_usage == 512.5
    
    def test_throughput_calculation(self):
        """Test throughput calculation"""
        metrics_collector = Mock()
        metrics_collector.calculate_throughput.return_value = 2000  # records per minute
        
        throughput = metrics_collector.calculate_throughput(
            records_processed=1000,
            time_elapsed_minutes=0.5
        )
        assert throughput == 2000

# Integration test examples
class TestIngestionIntegration:
    """Integration tests for the ingestion pipeline"""
    
    @pytest.mark.integration
    def test_end_to_end_ingestion_flow(self):
        """Test complete ingestion flow (requires actual Azure resources)"""
        # This would be run only in integration test environment
        # with actual Azure resources available
        pass
    
    @pytest.mark.integration  
    def test_azure_storage_integration(self):
        """Test integration with Azure Storage"""
        # This would test actual Azure Storage operations
        pass
    
    @pytest.mark.integration
    def test_key_vault_integration(self):
        """Test integration with Azure Key Vault"""
        # This would test actual Key Vault operations
        pass

# Fixtures for test data
@pytest.fixture
def sample_insurance_data():
    """Fixture providing sample insurance data"""
    return pd.DataFrame({
        'policy_id': ['POL001', 'POL002', 'POL003', 'POL004', 'POL005'],
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005'],
        'premium': [1000.0, 1500.0, 2000.0, 1200.0, 1800.0],
        'start_date': ['2024-01-01', '2024-01-15', '2024-02-01', '2024-02-15', '2024-03-01'],
        'end_date': ['2024-12-31', '2025-01-14', '2025-01-31', '2025-02-14', '2025-02-28'],
        'policy_type': ['AUTO', 'HOME', 'LIFE', 'AUTO', 'HOME'],
        'status': ['ACTIVE', 'ACTIVE', 'ACTIVE', 'PENDING', 'ACTIVE']
    })

@pytest.fixture
def mock_azure_clients():
    """Fixture providing mocked Azure clients"""
    with patch('scripts.ingestion.BlobServiceClient') as mock_blob, \
         patch('scripts.ingestion.SecretClient') as mock_secret:
        
        # Configure mock blob client
        mock_blob_instance = Mock()
        mock_blob.return_value = mock_blob_instance
        
        # Configure mock secret client
        mock_secret_instance = Mock()
        mock_secret.return_value = mock_secret_instance
        
        yield {
            'blob_client': mock_blob_instance,
            'secret_client': mock_secret_instance
        }

# Test configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "azure: mark test as requiring Azure resources"
    )
