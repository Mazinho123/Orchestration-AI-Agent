"""
Monitoring and metrics utilities.
"""
import time
from typing import Dict, Any, Optional
from functools import wraps
import asyncio
import logging

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Simple metrics collector for monitoring system performance.
    
    In production, integrate with:
    - Prometheus
    - Grafana
    - DataDog
    - New Relic
    """
    
    def __init__(self):
        self._metrics: Dict[str, list] = {
            "task_duration": [],
            "step_duration": [],
            "queue_depth": [],
            "errors": []
        }
    
    def record_task_duration(self, task_id: str, duration: float) -> None:
        """Record task execution duration."""
        self._metrics["task_duration"].append({
            "task_id": task_id,
            "duration": duration,
            "timestamp": time.time()
        })
        logger.info(f"Task {task_id} completed in {duration:.2f}s")
    
    def record_step_duration(
        self,
        task_id: str,
        step_id: str,
        duration: float
    ) -> None:
        """Record step execution duration."""
        self._metrics["step_duration"].append({
            "task_id": task_id,
            "step_id": step_id,
            "duration": duration,
            "timestamp": time.time()
        })
    
    def record_error(
        self,
        error_type: str,
        error_message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record error occurrence."""
        self._metrics["errors"].append({
            "type": error_type,
            "message": error_message,
            "context": context or {},
            "timestamp": time.time()
        })
        logger.error(f"Error recorded: {error_type} - {error_message}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics."""
        stats = {}
        
        if self._metrics["task_duration"]:
            durations = [m["duration"] for m in self._metrics["task_duration"]]
            stats["task_duration"] = {
                "count": len(durations),
                "avg": sum(durations) / len(durations),
                "min": min(durations),
                "max": max(durations)
            }
        
        stats["total_errors"] = len(self._metrics["errors"])
        
        return stats


# Global metrics collector
metrics = MetricsCollector()


def timing_decorator(metric_name: str):
    """
    Decorator to measure execution time of async functions.
    
    Args:
        metric_name: Name of the metric to record
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                logger.debug(f"{metric_name}: {duration:.3f}s")
        return wrapper
    return decorator
