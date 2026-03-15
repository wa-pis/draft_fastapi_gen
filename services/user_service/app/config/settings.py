from functools import lru_cache
from pydantic import BaseSettings, AnyUrl


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

    # kafka
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_CLIENT_ID: str = "user-service"

    # redis
    REDIS_URL: AnyUrl

    # telemetry
    OTLP_ENDPOINT: str | None = None
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()

