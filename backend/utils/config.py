"""Central configuration management using Pydantic Settings."""
from __future__ import annotations

from functools import cached_property
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Monitored databases. Stored as a raw comma-separated string so pydantic-settings
    # does NOT try to JSON-decode it (which happens for list-typed env fields). The
    # parsed list is exposed through the ``monitored_dbs`` cached property below.
    monitored_dbs_raw: str = Field(default="", alias="MONITORED_DBS")

    # Agent's own store (TimescaleDB)
    timescale_url: str = "postgresql://postgres:password@localhost:5433/dba_metrics"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # LLM
    llm_provider: str = "ollama"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "deepseek-coder:33b"
    groq_api_key: str = ""
    groq_model: str = "llama3-70b-8192"

    # Notifications
    slack_webhook_url: str = ""
    slack_channel: str = "#dba-alerts"

    # Auth
    jwt_secret: str = "change-me"

    # Monitoring intervals (seconds)
    critical_poll_interval: int = 10
    secondary_poll_interval: int = 60
    forecast_update_interval: int = 3600

    # Thresholds
    slow_query_threshold_ms: int = 1000
    p99_regression_threshold: float = 0.10
    max_monitoring_overhead_pct: float = 2.0
    capacity_warning_days: int = 28

    # Shadow DB
    shadow_db_image: str = "postgres:16-alpine"
    shadow_mysql_image: str = "mysql:8.0"
    max_concurrent_replays: int = 10

    # MinIO / cold storage
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    cold_storage_bucket: str = "dba-metrics-cold"

    # App
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    environment: str = "development"

    @cached_property
    def monitored_dbs(self) -> List[str]:
        """Parsed list of monitored database connection URLs."""
        return [db.strip() for db in self.monitored_dbs_raw.split(",") if db.strip()]


settings = Settings()