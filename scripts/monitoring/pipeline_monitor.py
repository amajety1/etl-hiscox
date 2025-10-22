"""
Pipeline monitoring and observability module for Hiscox ETL Pipeline
"""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

@dataclass
class PipelineMetrics:
    """Data class for pipeline metrics"""
    pipeline_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    records_processed: int = 0
    errors_count: int = 0
    warnings_count: int = 0
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None

class PipelineMonitor:
    """Pipeline monitoring and observability class"""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.logger = structlog.get_logger(__name__)
        self.tracer = trace.get_tracer(__name__)
        
        # Configure Azure Monitor if connection string provided
        if connection_string:
            configure_azure_monitor(connection_string=connection_string)
            RequestsInstrumentor().instrument()
            self.logger.info("Azure Monitor configured", connection_string_provided=True)
        
        self.active_runs: Dict[str, PipelineMetrics] = {}
        
    def start_pipeline_monitoring(self, pipeline_name: str, run_id: str, **kwargs) -> PipelineMetrics:
        """Start monitoring a pipeline run"""
        
        metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
            run_id=run_id,
            start_time=datetime.utcnow(),
            status="running"
        )
        
        self.active_runs[run_id] = metrics
        
        with self.tracer.start_as_current_span("pipeline_start") as span:
            span.set_attribute("pipeline.name", pipeline_name)
            span.set_attribute("pipeline.run_id", run_id)
            span.set_attribute("pipeline.start_time", metrics.start_time.isoformat())
            
            # Add custom attributes
            for key, value in kwargs.items():
                span.set_attribute(f"pipeline.{key}", str(value))
        
        self.logger.info(
            "Pipeline monitoring started",
            pipeline_name=pipeline_name,
            run_id=run_id,
            start_time=metrics.start_time.isoformat(),
            **kwargs
        )
        
        return metrics
    
    def update_pipeline_metrics(self, run_id: str, **metrics_update):
        """Update metrics for a running pipeline"""
        
        if run_id not in self.active_runs:
            self.logger.warning("Attempted to update metrics for unknown run_id", run_id=run_id)
            return
        
        metrics = self.active_runs[run_id]
        
        # Update metrics
        for key, value in metrics_update.items():
            if hasattr(metrics, key):
                setattr(metrics, key, value)
        
        with self.tracer.start_as_current_span("pipeline_metrics_update") as span:
            span.set_attribute("pipeline.run_id", run_id)
            for key, value in metrics_update.items():
                span.set_attribute(f"pipeline.{key}", str(value))
        
        self.logger.info(
            "Pipeline metrics updated",
            run_id=run_id,
            pipeline_name=metrics.pipeline_name,
            **metrics_update
        )
    
    def complete_pipeline_monitoring(self, run_id: str, status: str = "completed", **final_metrics):
        """Complete monitoring for a pipeline run"""
        
        if run_id not in self.active_runs:
            self.logger.warning("Attempted to complete monitoring for unknown run_id", run_id=run_id)
            return
        
        metrics = self.active_runs[run_id]
        metrics.end_time = datetime.utcnow()
        metrics.status = status
        metrics.duration_seconds = (metrics.end_time - metrics.start_time).total_seconds()
        
        # Update with final metrics
        for key, value in final_metrics.items():
            if hasattr(metrics, key):
                setattr(metrics, key, value)
        
        with self.tracer.start_as_current_span("pipeline_completion") as span:
            span.set_attribute("pipeline.name", metrics.pipeline_name)
            span.set_attribute("pipeline.run_id", run_id)
            span.set_attribute("pipeline.status", status)
            span.set_attribute("pipeline.duration_seconds", metrics.duration_seconds)
            span.set_attribute("pipeline.records_processed", metrics.records_processed)
            span.set_attribute("pipeline.errors_count", metrics.errors_count)
        
        self.logger.info(
            "Pipeline monitoring completed",
            pipeline_name=metrics.pipeline_name,
            run_id=run_id,
            status=status,
            duration_seconds=metrics.duration_seconds,
            records_processed=metrics.records_processed,
            errors_count=metrics.errors_count,
            warnings_count=metrics.warnings_count,
            **final_metrics
        )
        
        # Remove from active runs
        completed_metrics = self.active_runs.pop(run_id)
        return completed_metrics
    
    def log_error(self, run_id: str, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log an error for a pipeline run"""
        
        if run_id in self.active_runs:
            self.active_runs[run_id].errors_count += 1
        
        with self.tracer.start_as_current_span("pipeline_error") as span:
            span.set_attribute("pipeline.run_id", run_id)
            span.set_attribute("error.type", type(error).__name__)
            span.set_attribute("error.message", str(error))
            span.record_exception(error)
        
        self.logger.error(
            "Pipeline error occurred",
            run_id=run_id,
            error_type=type(error).__name__,
            error_message=str(error),
            context=context or {}
        )
    
    def log_warning(self, run_id: str, message: str, context: Optional[Dict[str, Any]] = None):
        """Log a warning for a pipeline run"""
        
        if run_id in self.active_runs:
            self.active_runs[run_id].warnings_count += 1
        
        with self.tracer.start_as_current_span("pipeline_warning") as span:
            span.set_attribute("pipeline.run_id", run_id)
            span.set_attribute("warning.message", message)
        
        self.logger.warning(
            "Pipeline warning",
            run_id=run_id,
            warning_message=message,
            context=context or {}
        )
    
    def get_active_runs(self) -> List[PipelineMetrics]:
        """Get all currently active pipeline runs"""
        return list(self.active_runs.values())
    
    def get_run_metrics(self, run_id: str) -> Optional[PipelineMetrics]:
        """Get metrics for a specific run"""
        return self.active_runs.get(run_id)
    
    def export_metrics_json(self, run_id: str) -> Optional[str]:
        """Export metrics as JSON string"""
        
        metrics = self.active_runs.get(run_id)
        if not metrics:
            return None
        
        metrics_dict = {
            "pipeline_name": metrics.pipeline_name,
            "run_id": metrics.run_id,
            "start_time": metrics.start_time.isoformat(),
            "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
            "status": metrics.status,
            "records_processed": metrics.records_processed,
            "errors_count": metrics.errors_count,
            "warnings_count": metrics.warnings_count,
            "duration_seconds": metrics.duration_seconds,
            "memory_usage_mb": metrics.memory_usage_mb,
            "cpu_usage_percent": metrics.cpu_usage_percent
        }
        
        return json.dumps(metrics_dict, indent=2)

class PerformanceProfiler:
    """Performance profiling utilities"""
    
    def __init__(self, monitor: PipelineMonitor):
        self.monitor = monitor
        self.logger = structlog.get_logger(__name__)
    
    def profile_function(self, run_id: str, function_name: str):
        """Decorator to profile function execution"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                
                with self.monitor.tracer.start_as_current_span(f"function_{function_name}") as span:
                    span.set_attribute("function.name", function_name)
                    span.set_attribute("pipeline.run_id", run_id)
                    
                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("function.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("function.status", "error")
                        span.record_exception(e)
                        self.monitor.log_error(run_id, e, {"function": function_name})
                        raise
                    finally:
                        execution_time = time.time() - start_time
                        span.set_attribute("function.duration_seconds", execution_time)
                        
                        self.logger.info(
                            "Function execution completed",
                            run_id=run_id,
                            function_name=function_name,
                            duration_seconds=execution_time
                        )
            
            return wrapper
        return decorator

class AlertManager:
    """Alert management for pipeline monitoring"""
    
    def __init__(self, monitor: PipelineMonitor, webhook_url: Optional[str] = None):
        self.monitor = monitor
        self.webhook_url = webhook_url
        self.logger = structlog.get_logger(__name__)
        
        # Alert thresholds
        self.thresholds = {
            "max_duration_minutes": 60,
            "max_error_rate": 0.05,
            "max_memory_usage_mb": 4096,
            "max_cpu_usage_percent": 90
        }
    
    def check_alerts(self, run_id: str):
        """Check if any alerts should be triggered"""
        
        metrics = self.monitor.get_run_metrics(run_id)
        if not metrics:
            return
        
        alerts = []
        
        # Duration alert
        if metrics.duration_seconds and metrics.duration_seconds > (self.thresholds["max_duration_minutes"] * 60):
            alerts.append({
                "type": "duration",
                "message": f"Pipeline {metrics.pipeline_name} has been running for {metrics.duration_seconds/60:.1f} minutes",
                "severity": "warning"
            })
        
        # Error rate alert
        if metrics.records_processed > 0:
            error_rate = metrics.errors_count / metrics.records_processed
            if error_rate > self.thresholds["max_error_rate"]:
                alerts.append({
                    "type": "error_rate",
                    "message": f"High error rate: {error_rate:.2%} in pipeline {metrics.pipeline_name}",
                    "severity": "critical"
                })
        
        # Memory usage alert
        if metrics.memory_usage_mb and metrics.memory_usage_mb > self.thresholds["max_memory_usage_mb"]:
            alerts.append({
                "type": "memory",
                "message": f"High memory usage: {metrics.memory_usage_mb:.1f}MB in pipeline {metrics.pipeline_name}",
                "severity": "warning"
            })
        
        # CPU usage alert
        if metrics.cpu_usage_percent and metrics.cpu_usage_percent > self.thresholds["max_cpu_usage_percent"]:
            alerts.append({
                "type": "cpu",
                "message": f"High CPU usage: {metrics.cpu_usage_percent:.1f}% in pipeline {metrics.pipeline_name}",
                "severity": "warning"
            })
        
        # Send alerts
        for alert in alerts:
            self.send_alert(run_id, alert)
    
    def send_alert(self, run_id: str, alert: Dict[str, Any]):
        """Send an alert notification"""
        
        self.logger.warning(
            "Alert triggered",
            run_id=run_id,
            alert_type=alert["type"],
            alert_message=alert["message"],
            severity=alert["severity"]
        )
        
        # Send to webhook if configured
        if self.webhook_url:
            try:
                import requests
                
                payload = {
                    "text": f"🚨 {alert['severity'].upper()}: {alert['message']}",
                    "run_id": run_id,
                    "alert_type": alert["type"],
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                response = requests.post(self.webhook_url, json=payload, timeout=10)
                response.raise_for_status()
                
                self.logger.info("Alert sent successfully", run_id=run_id, alert_type=alert["type"])
                
            except Exception as e:
                self.logger.error("Failed to send alert", run_id=run_id, error=str(e))

# Example usage
if __name__ == "__main__":
    import os
    
    # Initialize monitor
    connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    monitor = PipelineMonitor(connection_string)
    
    # Initialize alert manager
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    alert_manager = AlertManager(monitor, webhook_url)
    
    # Example pipeline monitoring
    run_id = "test-run-001"
    
    # Start monitoring
    metrics = monitor.start_pipeline_monitoring("test-pipeline", run_id, environment="dev")
    
    # Simulate some work
    time.sleep(1)
    monitor.update_pipeline_metrics(run_id, records_processed=1000)
    
    # Complete monitoring
    monitor.complete_pipeline_monitoring(run_id, "completed", records_processed=5000)
    
    print("Monitoring example completed")
