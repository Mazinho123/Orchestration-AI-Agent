"""
Central configuration module for the Agentic AI System.
Loads environment variables and provides type-safe configuration.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Redis Configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    
    # LLM Configuration
    llm_api_base: str = "https://api.together.xyz/v1"
    llm_api_key: str = ""
    llm_model: str = "mistralai/Mixtral-8x7B-Instruct-v0.1"
    
    # Queue Configuration
    queue_retry_max_attempts: int = 3
    queue_retry_backoff_base: int = 2
    queue_message_ttl: int = 3600
    
    # Agent Timeouts (seconds)
    planner_timeout: int = 30
    retriever_timeout: int = 20
    analyzer_timeout: int = 25
    writer_timeout: int = 40
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: str = "*"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
