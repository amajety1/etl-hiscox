"""
Data quality tests for Hiscox insurance data
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
import re

class TestInsuranceDataQuality:
    """Data quality tests for insurance data"""
    
    @pytest.fixture
    def sample_insurance_data(self):
        """Sample insurance data for testing"""
        return pd.DataFrame({
            'policy_id': ['POL001', 'POL002', 'POL003', 'POL004', 'POL005'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005'],
            'premium': [1000.0, 1500.0, 2000.0, 1200.0, 1800.0],
            'start_date': ['2024-01-01', '2024-01-15', '2024-02-01', '2024-02-15', '2024-03-01'],
            'end_date': ['2024-12-31', '2025-01-14', '2025-01-31', '2025-02-14', '2025-02-28'],
            'policy_type': ['AUTO', 'HOME', 'LIFE', 'AUTO', 'HOME'],
            'status': ['ACTIVE', 'ACTIVE', 'ACTIVE', 'PENDING', 'ACTIVE'],
            'coverage_amount': [50000.0, 200000.0, 100000.0, 75000.0, 150000.0],
            'deductible': [500.0, 1000.0, 0.0, 500.0, 1000.0],
            'agent_id': ['AGT001', 'AGT002', 'AGT001', 'AGT003', 'AGT002']
        })
    
    def test_policy_id_format(self, sample_insurance_data):
        """Test that policy IDs follow the correct format"""
        policy_pattern = re.compile(r'^POL\d{3,}$')
        
        for policy_id in sample_insurance_data['policy_id']:
            assert policy_pattern.match(policy_id), f"Invalid policy ID format: {policy_id}"
    
    def test_policy_id_uniqueness(self, sample_insurance_data):
        """Test that policy IDs are unique"""
        policy_ids = sample_insurance_data['policy_id']
        assert len(policy_ids) == len(policy_ids.unique()), "Policy IDs are not unique"
    
    def test_customer_id_format(self, sample_insurance_data):
        """Test that customer IDs follow the correct format"""
        customer_pattern = re.compile(r'^CUST\d{3,}$')
        
        for customer_id in sample_insurance_data['customer_id']:
            assert customer_pattern.match(customer_id), f"Invalid customer ID format: {customer_id}"
    
    def test_premium_values(self, sample_insurance_data):
        """Test premium value constraints"""
        premiums = sample_insurance_data['premium']
        
        # All premiums should be positive
        assert (premiums > 0).all(), "Found non-positive premium values"
        
        # Premiums should be within reasonable range
        assert (premiums >= 100).all(), "Found premiums below minimum threshold"
        assert (premiums <= 100000).all(), "Found premiums above maximum threshold"
        
        # No null values
        assert not premiums.isnull().any(), "Found null premium values"
    
    def test_date_formats_and_validity(self, sample_insurance_data):
        """Test date formats and validity"""
        # Convert to datetime
        start_dates = pd.to_datetime(sample_insurance_data['start_date'])
        end_dates = pd.to_datetime(sample_insurance_data['end_date'])
        
        # No null dates
        assert not start_dates.isnull().any(), "Found null start dates"
        assert not end_dates.isnull().any(), "Found null end dates"
        
        # End dates should be after start dates
        assert (end_dates > start_dates).all(), "Found end dates before start dates"
        
        # Dates should be within reasonable range
        min_date = datetime(2020, 1, 1)
        max_date = datetime(2030, 12, 31)
        
        assert (start_dates >= min_date).all(), "Found start dates before minimum allowed date"
        assert (end_dates <= max_date).all(), "Found end dates after maximum allowed date"
    
    def test_policy_type_values(self, sample_insurance_data):
        """Test policy type values"""
        valid_policy_types = ['AUTO', 'HOME', 'LIFE', 'HEALTH', 'BUSINESS']
        policy_types = sample_insurance_data['policy_type']
        
        # All policy types should be valid
        for policy_type in policy_types:
            assert policy_type in valid_policy_types, f"Invalid policy type: {policy_type}"
        
        # No null values
        assert not policy_types.isnull().any(), "Found null policy types"
    
    def test_status_values(self, sample_insurance_data):
        """Test status values"""
        valid_statuses = ['ACTIVE', 'PENDING', 'CANCELLED', 'EXPIRED', 'SUSPENDED']
        statuses = sample_insurance_data['status']
        
        # All statuses should be valid
        for status in statuses:
            assert status in valid_statuses, f"Invalid status: {status}"
        
        # No null values
        assert not statuses.isnull().any(), "Found null status values"
    
    def test_coverage_amount_values(self, sample_insurance_data):
        """Test coverage amount values"""
        coverage_amounts = sample_insurance_data['coverage_amount']
        
        # All coverage amounts should be positive
        assert (coverage_amounts > 0).all(), "Found non-positive coverage amounts"
        
        # Coverage amounts should be within reasonable range
        assert (coverage_amounts >= 1000).all(), "Found coverage amounts below minimum threshold"
        assert (coverage_amounts <= 10000000).all(), "Found coverage amounts above maximum threshold"
        
        # No null values
        assert not coverage_amounts.isnull().any(), "Found null coverage amounts"
    
    def test_deductible_values(self, sample_insurance_data):
        """Test deductible values"""
        deductibles = sample_insurance_data['deductible']
        
        # Deductibles should be non-negative (can be 0)
        assert (deductibles >= 0).all(), "Found negative deductible values"
        
        # Deductibles should be reasonable compared to coverage
        coverage_amounts = sample_insurance_data['coverage_amount']
        deductible_ratio = deductibles / coverage_amounts
        assert (deductible_ratio <= 0.5).all(), "Found deductibles exceeding 50% of coverage amount"
        
        # No null values
        assert not deductibles.isnull().any(), "Found null deductible values"
    
    def test_agent_id_format(self, sample_insurance_data):
        """Test agent ID format"""
        agent_pattern = re.compile(r'^AGT\d{3,}$')
        
        for agent_id in sample_insurance_data['agent_id']:
            assert agent_pattern.match(agent_id), f"Invalid agent ID format: {agent_id}"
    
    def test_data_completeness(self, sample_insurance_data):
        """Test overall data completeness"""
        required_columns = [
            'policy_id', 'customer_id', 'premium', 'start_date', 
            'end_date', 'policy_type', 'status'
        ]
        
        for column in required_columns:
            assert column in sample_insurance_data.columns, f"Missing required column: {column}"
            
            # Check for null values in required columns
            null_count = sample_insurance_data[column].isnull().sum()
            assert null_count == 0, f"Found {null_count} null values in required column: {column}"
    
    def test_business_rule_validations(self, sample_insurance_data):
        """Test business rule validations"""
        # Life insurance policies should have higher coverage amounts
        life_policies = sample_insurance_data[sample_insurance_data['policy_type'] == 'LIFE']
        if not life_policies.empty:
            assert (life_policies['coverage_amount'] >= 50000).all(), \
                "Life insurance policies should have minimum $50,000 coverage"
        
        # Auto insurance should have deductibles
        auto_policies = sample_insurance_data[sample_insurance_data['policy_type'] == 'AUTO']
        if not auto_policies.empty:
            assert (auto_policies['deductible'] > 0).all(), \
                "Auto insurance policies should have deductibles"
        
        # Active policies should have future end dates
        active_policies = sample_insurance_data[sample_insurance_data['status'] == 'ACTIVE']
        if not active_policies.empty:
            end_dates = pd.to_datetime(active_policies['end_date'])
            current_date = datetime.now()
            assert (end_dates > current_date).all(), \
                "Active policies should have future end dates"

class TestDataQualityMetrics:
    """Test data quality metrics calculation"""
    
    def test_completeness_score(self):
        """Test completeness score calculation"""
        # Sample data with some missing values
        data = pd.DataFrame({
            'col1': [1, 2, None, 4, 5],
            'col2': ['A', 'B', 'C', None, 'E'],
            'col3': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        
        # Calculate completeness score
        total_cells = data.size
        non_null_cells = data.count().sum()
        completeness_score = non_null_cells / total_cells
        
        expected_score = 13 / 15  # 13 non-null values out of 15 total
        assert abs(completeness_score - expected_score) < 0.001
    
    def test_uniqueness_score(self):
        """Test uniqueness score calculation"""
        # Sample data with duplicates
        data = pd.Series([1, 2, 2, 3, 4, 4, 5])
        
        unique_count = data.nunique()
        total_count = len(data)
        uniqueness_score = unique_count / total_count
        
        expected_score = 5 / 7  # 5 unique values out of 7 total
        assert abs(uniqueness_score - expected_score) < 0.001
    
    def test_validity_score(self):
        """Test validity score calculation"""
        # Sample email data
        emails = pd.Series([
            'valid@email.com',
            'also.valid@domain.org',
            'invalid-email',
            'another@valid.email.co.uk',
            'not-an-email'
        ])
        
        # Simple email validation pattern
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        valid_emails = emails.apply(lambda x: bool(email_pattern.match(str(x))))
        
        validity_score = valid_emails.sum() / len(emails)
        expected_score = 3 / 5  # 3 valid emails out of 5
        
        assert abs(validity_score - expected_score) < 0.001

class TestDataQualityRules:
    """Test custom data quality rules"""
    
    def test_premium_to_coverage_ratio(self):
        """Test premium to coverage ratio rule"""
        data = pd.DataFrame({
            'premium': [1000, 2000, 500],
            'coverage_amount': [50000, 100000, 25000]
        })
        
        # Calculate premium to coverage ratio
        ratio = data['premium'] / data['coverage_amount']
        
        # Rule: Premium should be between 1% and 10% of coverage amount
        valid_ratio = (ratio >= 0.01) & (ratio <= 0.10)
        
        assert valid_ratio.all(), "Premium to coverage ratio outside acceptable range"
    
    def test_policy_duration_rule(self):
        """Test policy duration rule"""
        data = pd.DataFrame({
            'start_date': ['2024-01-01', '2024-06-01', '2024-12-01'],
            'end_date': ['2024-12-31', '2025-05-31', '2025-11-30'],
            'policy_type': ['AUTO', 'HOME', 'LIFE']
        })
        
        start_dates = pd.to_datetime(data['start_date'])
        end_dates = pd.to_datetime(data['end_date'])
        duration_days = (end_dates - start_dates).dt.days
        
        # Rule: Policies should be at least 30 days and at most 5 years
        min_duration = 30
        max_duration = 365 * 5
        
        valid_duration = (duration_days >= min_duration) & (duration_days <= max_duration)
        assert valid_duration.all(), "Policy duration outside acceptable range"
    
    def test_age_based_premium_rule(self):
        """Test age-based premium rule"""
        data = pd.DataFrame({
            'customer_age': [25, 45, 65, 30, 55],
            'premium': [800, 1200, 2000, 900, 1500],
            'policy_type': ['AUTO', 'AUTO', 'AUTO', 'AUTO', 'AUTO']
        })
        
        # Rule: For auto insurance, older customers should generally have higher premiums
        # This is a simplified rule for demonstration
        auto_data = data[data['policy_type'] == 'AUTO'].copy()
        auto_data = auto_data.sort_values('customer_age')
        
        # Check if premium generally increases with age (allowing some variance)
        premium_trend = auto_data['premium'].rolling(window=2).apply(
            lambda x: x.iloc[1] >= x.iloc[0] * 0.8  # Allow 20% decrease
        ).fillna(True)
        
        # At least 80% should follow the trend
        trend_compliance = premium_trend.mean()
        assert trend_compliance >= 0.8, f"Age-based premium trend compliance: {trend_compliance}"

class TestDataQualityReporting:
    """Test data quality reporting functionality"""
    
    def test_generate_quality_report(self):
        """Test generation of data quality report"""
        # Sample data with various quality issues
        data = pd.DataFrame({
            'policy_id': ['POL001', 'POL002', None, 'POL004', 'POL002'],  # Null and duplicate
            'premium': [1000, -500, 2000, 1500, 1200],  # Negative value
            'email': ['valid@email.com', 'invalid-email', 'another@valid.com', None, 'test@domain.org']
        })
        
        # Calculate quality metrics
        quality_report = {
            'total_records': len(data),
            'completeness': {},
            'uniqueness': {},
            'validity': {}
        }
        
        for column in data.columns:
            # Completeness
            non_null_count = data[column].count()
            quality_report['completeness'][column] = non_null_count / len(data)
            
            # Uniqueness
            unique_count = data[column].nunique()
            quality_report['uniqueness'][column] = unique_count / non_null_count if non_null_count > 0 else 0
        
        # Validity for specific columns
        if 'premium' in data.columns:
            valid_premiums = (data['premium'] > 0).sum()
            quality_report['validity']['premium'] = valid_premiums / data['premium'].count()
        
        if 'email' in data.columns:
            email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            valid_emails = data['email'].dropna().apply(lambda x: bool(email_pattern.match(str(x)))).sum()
            quality_report['validity']['email'] = valid_emails / data['email'].count()
        
        # Assertions
        assert quality_report['total_records'] == 5
        assert quality_report['completeness']['policy_id'] == 0.8  # 4/5
        assert quality_report['uniqueness']['policy_id'] == 0.75   # 3/4 (excluding null)
        assert quality_report['validity']['premium'] == 0.75       # 3/4 positive values
        assert quality_report['validity']['email'] == 0.75        # 3/4 valid emails
    
    def test_quality_threshold_alerts(self):
        """Test quality threshold alerting"""
        quality_metrics = {
            'completeness': 0.85,
            'uniqueness': 0.90,
            'validity': 0.75
        }
        
        thresholds = {
            'completeness': 0.90,
            'uniqueness': 0.95,
            'validity': 0.80
        }
        
        alerts = []
        for metric, value in quality_metrics.items():
            if value < thresholds[metric]:
                alerts.append({
                    'metric': metric,
                    'value': value,
                    'threshold': thresholds[metric],
                    'severity': 'warning' if value >= thresholds[metric] * 0.9 else 'critical'
                })
        
        # Should have alerts for all metrics
        assert len(alerts) == 3
        
        # Check specific alerts
        completeness_alert = next(a for a in alerts if a['metric'] == 'completeness')
        assert completeness_alert['severity'] == 'warning'  # 0.85 >= 0.90 * 0.9
        
        uniqueness_alert = next(a for a in alerts if a['metric'] == 'uniqueness')
        assert uniqueness_alert['severity'] == 'critical'  # 0.90 < 0.95 * 0.9

# Fixtures for data quality tests
@pytest.fixture
def problematic_insurance_data():
    """Fixture with insurance data containing quality issues"""
    return pd.DataFrame({
        'policy_id': ['POL001', '', 'POL003', 'POL001', 'POL005'],  # Empty and duplicate
        'customer_id': ['CUST001', 'CUST002', None, 'CUST004', 'CUST005'],  # Null value
        'premium': [1000.0, -500.0, 2000.0, 1500.0, 0.0],  # Negative and zero
        'start_date': ['2024-01-01', '2024-01-15', 'invalid-date', '2024-02-15', '2024-03-01'],  # Invalid date
        'end_date': ['2024-12-31', '2023-01-14', '2025-01-31', '2025-02-14', '2025-02-28'],  # End before start
        'policy_type': ['AUTO', 'UNKNOWN', 'LIFE', 'AUTO', ''],  # Invalid and empty
        'status': ['ACTIVE', 'ACTIVE', 'INVALID_STATUS', 'PENDING', 'ACTIVE'],  # Invalid status
        'coverage_amount': [50000.0, 200000.0, -100000.0, 75000.0, 150000.0],  # Negative value
        'email': ['valid@email.com', 'invalid-email', 'another@valid.com', None, 'test@domain']  # Invalid email
    })

@pytest.fixture
def data_quality_thresholds():
    """Fixture with data quality thresholds"""
    return {
        'completeness_threshold': 0.95,
        'uniqueness_threshold': 0.98,
        'validity_threshold': 0.90,
        'consistency_threshold': 0.95
    }

# Parametrized tests for different policy types
@pytest.mark.parametrize("policy_type,min_coverage,max_premium_ratio", [
    ("AUTO", 25000, 0.05),
    ("HOME", 100000, 0.03),
    ("LIFE", 50000, 0.10)
])
def test_policy_type_specific_rules(policy_type, min_coverage, max_premium_ratio):
    """Test policy type specific business rules"""
    data = pd.DataFrame({
        'policy_type': [policy_type],
        'coverage_amount': [min_coverage + 10000],
        'premium': [(min_coverage + 10000) * max_premium_ratio * 0.8]  # Within limit
    })
    
    # Test minimum coverage
    assert data['coverage_amount'].iloc[0] >= min_coverage
    
    # Test premium ratio
    premium_ratio = data['premium'].iloc[0] / data['coverage_amount'].iloc[0]
    assert premium_ratio <= max_premium_ratio
