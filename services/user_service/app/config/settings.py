from functools import lru_cache

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # service
    SERVICE_NAME: str = "user-service"
    ENV: str = "local"
    DEBUG: bool = True

    # http
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # database
    DATABASE_URL: AnyUrl
    DB_POOL_SIZE: int = Field(default=5, ge=1)
    DB_MAX_OVERFLOW: int = Field(default=10, ge=0)
    DB_POOL_TIMEOUT_SECONDS: float = Field(default=30.0, gt=0)
    DB_POOL_RECYCLE_SECONDS: int = Field(default=1800, ge=0)

    # kafka
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_CLIENT_ID: str = "user-service"
    KAFKA_HEALTH_TTL_SECONDS: float = Field(default=10.0, gt=0)
    KAFKA_HEALTH_TIMEOUT_SECONDS: float = Field(default=3.0, gt=0)

    # redis
    REDIS_URL: AnyUrl
    IDEMPOTENCY_TTL_SECONDS: int = Field(default=3600, gt=0)
    IDEMPOTENCY_LOCK_TTL_SECONDS: int = Field(default=30, gt=0)

    # telemetry
    OTLP_ENDPOINT: str | None = None
    LOG_LEVEL: str = "INFO"

    # outbox
    OUTBOX_BATCH_SIZE: int = Field(default=100, ge=1)
    OUTBOX_IDLE_SLEEP_SECONDS: float = Field(default=0.5, ge=0)
    OUTBOX_PUBLISH_CONCURRENCY: int = Field(default=10, ge=1)
    OUTBOX_RETRY_DELAY_SECONDS: int = Field(default=30, ge=1)
    OUTBOX_MAX_ATTEMPTS: int = Field(default=5, ge=1)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
