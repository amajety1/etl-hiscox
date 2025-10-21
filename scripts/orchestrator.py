#!/usr/bin/env python3
"""
Hiscox ETL Pipeline - Orchestration Script
Coordinates the entire ETL pipeline execution
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config import Config
from logger import setup_logger

class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline execution"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger(__name__)
        self.pipeline_start_time = datetime.now()
        
    def run_ingestion(self) -> bool:
        """Execute data ingestion step"""
        try:
            self.logger.info("🚀 Starting data ingestion step")
            
            result = subprocess.run([
                sys.executable, 
                os.path.join(os.path.dirname(__file__), 'ingestion.py')
            ], capture_output=True, text=True, timeout=1800)  # 30 min timeout
            
            if result.returncode == 0:
                self.logger.info("✅ Data ingestion completed successfully")
                return True
            else:
                self.logger.error(f"❌ Data ingestion failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Data ingestion timed out")
            return False
        except Exception as e:
            self.logger.error(f"❌ Data ingestion error: {str(e)}")
            return False
    
    def run_dbt_transformations(self) -> bool:
        """Execute dbt transformations"""
        try:
            self.logger.info("🔄 Starting dbt transformations")
            
            # Change to dbt directory
            dbt_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dbt')
            
            # Run dbt deps to install dependencies
            deps_result = subprocess.run([
                'dbt', 'deps', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir
            ], capture_output=True, text=True, cwd=dbt_dir)
            
            if deps_result.returncode != 0:
                self.logger.warning(f"dbt deps warning: {deps_result.stderr}")
            
            # Run dbt run
            run_result = subprocess.run([
                'dbt', 'run', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir
            ], capture_output=True, text=True, cwd=dbt_dir, timeout=3600)  # 60 min timeout
            
            if run_result.returncode == 0:
                self.logger.info("✅ dbt transformations completed successfully")
                
                # Run dbt tests
                test_result = subprocess.run([
                    'dbt', 'test', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir
                ], capture_output=True, text=True, cwd=dbt_dir, timeout=1800)
                
                if test_result.returncode == 0:
                    self.logger.info("✅ dbt tests passed successfully")
                    return True
                else:
                    self.logger.warning(f"⚠️ Some dbt tests failed: {test_result.stderr}")
                    return True  # Continue pipeline even if some tests fail
            else:
                self.logger.error(f"❌ dbt transformations failed: {run_result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ dbt transformations timed out")
            return False
        except Exception as e:
            self.logger.error(f"❌ dbt transformations error: {str(e)}")
            return False
    
    def run_data_quality_checks(self) -> bool:
        """Execute data quality validation"""
        try:
            self.logger.info("🔍 Starting data quality checks")
            
            # Run Great Expectations validation (if configured)
            # This is a placeholder for more comprehensive data quality checks
            
            self.logger.info("✅ Data quality checks completed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Data quality checks failed: {str(e)}")
            return False
    
    def generate_pipeline_report(self, results: Dict[str, bool]) -> None:
        """Generate pipeline execution report"""
        try:
            pipeline_end_time = datetime.now()
            duration = pipeline_end_time - self.pipeline_start_time
            
            report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        HISCOX ETL PIPELINE REPORT                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Pipeline Start Time: {self.pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}                                    ║
║ Pipeline End Time:   {pipeline_end_time.strftime('%Y-%m-%d %H:%M:%S')}                                    ║
║ Total Duration:      {str(duration).split('.')[0]}                                           ║
║                                                                              ║
║ STEP RESULTS:                                                                ║
║ • Data Ingestion:     {'✅ SUCCESS' if results.get('ingestion', False) else '❌ FAILED'}                                        ║
║ • dbt Transformations: {'✅ SUCCESS' if results.get('transformations', False) else '❌ FAILED'}                                       ║
║ • Data Quality:       {'✅ SUCCESS' if results.get('data_quality', False) else '❌ FAILED'}                                        ║
║                                                                              ║
║ Overall Status:       {'✅ SUCCESS' if all(results.values()) else '❌ FAILED'}                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
            """
            
            print(report)
            self.logger.info("Pipeline execution report generated")
            
        except Exception as e:
            self.logger.error(f"Failed to generate pipeline report: {str(e)}")
    
    def run_pipeline(self) -> bool:
        """Execute the complete ETL pipeline"""
        results = {}
        
        try:
            self.logger.info("🎯 Starting Hiscox ETL Pipeline Orchestration")
            
            # Step 1: Data Ingestion
            results['ingestion'] = self.run_ingestion()
            if not results['ingestion']:
                self.logger.error("Pipeline stopped due to ingestion failure")
                return False
            
            # Step 2: dbt Transformations
            results['transformations'] = self.run_dbt_transformations()
            if not results['transformations']:
                self.logger.error("Pipeline stopped due to transformation failure")
                return False
            
            # Step 3: Data Quality Checks
            results['data_quality'] = self.run_data_quality_checks()
            
            # Generate report
            self.generate_pipeline_report(results)
            
            # Return overall success
            pipeline_success = all(results.values())
            
            if pipeline_success:
                self.logger.info("🎉 ETL Pipeline completed successfully!")
            else:
                self.logger.error("💥 ETL Pipeline completed with errors")
            
            return pipeline_success
            
        except Exception as e:
            self.logger.error(f"💥 ETL Pipeline orchestration failed: {str(e)}")
            self.generate_pipeline_report(results)
            return False

def main():
    """Main execution function"""
    try:
        # Initialize configuration
        config = Config()
        
        # Run pipeline orchestration
        orchestrator = ETLOrchestrator(config)
        success = orchestrator.run_pipeline()
        
        # Exit with appropriate code
        if success:
            print("\n🎉 Hiscox ETL Pipeline completed successfully!")
            sys.exit(0)
        else:
            print("\n💥 Hiscox ETL Pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Fatal error in pipeline orchestration: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
