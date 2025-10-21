#!/usr/bin/env python3
"""
Logging utilities for Hiscox ETL Pipeline
"""

import os
import sys
import logging
import structlog
from datetime import datetime
from typing import Optional
from pathlib import Path

def setup_logger(name: str, log_level: str = "INFO", log_format: str = "json") -> structlog.BoundLogger:
    """
    Setup structured logger for the ETL pipeline
    
    Args:
        name: Logger name (usually __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log format (json or text)
    
    Returns:
        Configured structlog logger
    """
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging level
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure processors based on format
    if log_format.lower() == "json":
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ]
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer(colors=True)
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )
    
    # Create file handler for persistent logging
    log_file = log_dir / f"hiscox_etl_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    
    if log_format.lower() == "json":
        file_formatter = logging.Formatter('%(message)s')
    else:
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    file_handler.setFormatter(file_formatter)
    
    # Add file handler to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    
    # Create and return structlog logger
    logger = structlog.get_logger(name)
    
    # Add context information
    logger = logger.bind(
        service="hiscox-etl",
        environment=os.getenv("ENVIRONMENT", "dev"),
        version="1.0.0"
    )
    
    return logger

class ETLLogger:
    """Enhanced logger class with ETL-specific methods"""
    
    def __init__(self, name: str, log_level: str = "INFO", log_format: str = "json"):
        self.logger = setup_logger(name, log_level, log_format)
        self.start_time: Optional[datetime] = None
    
    def start_operation(self, operation: str, **kwargs) -> None:
        """Log the start of an operation"""
        self.start_time = datetime.now()
        self.logger.info(
            f"Starting {operation}",
            operation=operation,
            start_time=self.start_time.isoformat(),
            **kwargs
        )
    
    def end_operation(self, operation: str, success: bool = True, **kwargs) -> None:
        """Log the end of an operation"""
        end_time = datetime.now()
        duration = None
        
        if self.start_time:
            duration = (end_time - self.start_time).total_seconds()
        
        log_method = self.logger.info if success else self.logger.error
        log_method(
            f"{'Completed' if success else 'Failed'} {operation}",
            operation=operation,
            success=success,
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            **kwargs
        )
    
    def log_data_quality(self, table: str, total_rows: int, valid_rows: int, 
                        invalid_rows: int, **kwargs) -> None:
        """Log data quality metrics"""
        quality_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0
        
        self.logger.info(
            "Data quality check completed",
            table=table,
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            quality_rate_percent=round(quality_rate, 2),
            **kwargs
        )
    
    def log_performance(self, operation: str, records_processed: int, 
                       duration_seconds: float, **kwargs) -> None:
        """Log performance metrics"""
        records_per_second = records_processed / duration_seconds if duration_seconds > 0 else 0
        
        self.logger.info(
            "Performance metrics",
            operation=operation,
            records_processed=records_processed,
            duration_seconds=round(duration_seconds, 2),
            records_per_second=round(records_per_second, 2),
            **kwargs
        )
    
    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying logger"""
        return getattr(self.logger, name)
