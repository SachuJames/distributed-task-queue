"""Engine configuration (contract section 10).

All settings come from DTQ_-prefixed environment variables via pydantic-settings
and are validated on instantiation, so a bad value fails fast at startup.
"""

from __future__ import annotations

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class Settings(BaseSettings):
    """All engine tunables. Instantiate once at process startup."""

    model_config = SettingsConfigDict(env_prefix="DTQ_")

    redis_url: str = "redis://localhost:6379/0"
    api_host: str = "0.0.0.0"  # noqa: S104 - documented local-dev default bind
    api_port: int = Field(default=8000, ge=1, le=65535)
    ws_path: str = "/ws/events"

    worker_concurrency: int = Field(default=4, ge=1)
    worker_id: str = ""
    visibility_timeout_s: int = Field(default=30, ge=1)
    heartbeat_interval_s: int = Field(default=5, ge=1)

    max_queue_depth: int = Field(default=10000, ge=1)
    max_payload_bytes: int = Field(default=262144, ge=1)
    task_timeout_ms: int = Field(default=30000, ge=1, le=3_600_000)
    max_attempts: int = Field(default=10, ge=1)

    retry_base_delay: float = Field(default=1.0, gt=0)
    retry_max_delay: float = Field(default=60.0, gt=0)
    retry_jitter: float = Field(default=0.2, ge=0, le=1)

    task_retention_s: int = Field(default=604800, ge=1)
    result_retention_s: int = Field(default=3600, ge=1)
    result_max_bytes: int = Field(default=8192, ge=1)
    idem_ttl_s: int = Field(default=86400, ge=1)
    drain_timeout_s: int = Field(default=30, ge=1)

    dlq_enabled: bool = True
    dlq_maxlen: int = Field(default=10000, ge=1)

    api_key: str = ""
    log_level: str = "INFO"
    log_json: bool = False

    @field_validator("redis_url")
    @classmethod
    def _non_empty_redis_url(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("DTQ_REDIS_URL must not be empty")
        return value

    @field_validator("ws_path")
    @classmethod
    def _ws_path_leading_slash(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("DTQ_WS_PATH must start with '/'")
        return value

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in LOG_LEVELS:
            raise ValueError(f"DTQ_LOG_LEVEL must be one of {sorted(LOG_LEVELS)}")
        return normalized

    @model_validator(mode="after")
    def _cross_field_checks(self) -> Settings:
        if self.retry_max_delay < self.retry_base_delay:
            raise ValueError("DTQ_RETRY_MAX_DELAY must be >= DTQ_RETRY_BASE_DELAY")
        if self.heartbeat_interval_s >= self.visibility_timeout_s:
            raise ValueError(
                "DTQ_HEARTBEAT_INTERVAL_S must be < DTQ_VISIBILITY_TIMEOUT_S "
                "so heartbeats refresh before entries become reclaimable"
            )
        return self


def load_settings() -> Settings:
    """Build and validate settings, failing fast on bad values."""
    return Settings()
