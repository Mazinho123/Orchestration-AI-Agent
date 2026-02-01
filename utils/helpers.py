"""
Helper utilities for the Agentic AI System.
"""
import hashlib
import json
from typing import Any, Dict
from datetime import datetime


def generate_correlation_id(data: str) -> str:
    """
    Generate a correlation ID from data.
    
    Args:
        data: Input data
        
    Returns:
        Correlation ID
    """
    timestamp = datetime.utcnow().isoformat()
    combined = f"{data}:{timestamp}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def serialize_datetime(obj: Any) -> Any:
    """
    JSON serializer for datetime objects.
    
    Args:
        obj: Object to serialize
        
    Returns:
        Serialized object
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def deep_merge(dict1: Dict, dict2: Dict) -> Dict:
    """
    Deep merge two dictionaries.
    
    Args:
        dict1: First dictionary
        dict2: Second dictionary
        
    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to maximum length.
    
    Args:
        text: Input text
        max_length: Maximum length
        suffix: Suffix to add when truncated
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"
