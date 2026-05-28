from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    name: str = Field(default="kafka-async-service", alias="APP_NAME")
    environment: str = Field(default="local", alias="APP_ENV")


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    group_id: str = Field(default="kafka-async-service-group", alias="KAFKA_GROUP_ID")
    input_topic: str = Field(default="input-topic", alias="KAFKA_INPUT_TOPIC")
    output_topic: str | None = Field(default="output-topic", alias="KAFKA_OUTPUT_TOPIC")


class LoggingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    level: str = Field(default="INFO", alias="LOG_LEVEL")
    remote_enabled: bool = Field(default=True, alias="REMOTE_LOG_ENABLED")
    remote_host: str = Field(default="localhost", alias="REMOTE_LOG_HOST")
    remote_port: int = Field(default=5000, alias="REMOTE_LOG_PORT")
    remote_protocol: Literal["tcp", "udp"] = Field(default="tcp", alias="REMOTE_LOG_PROTOCOL")
    remote_timeout_seconds: float = Field(default=1.0, alias="REMOTE_LOG_TIMEOUT_SECONDS")


class OpenTelemetrySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    enabled: bool = Field(default=True, alias="OTEL_ENABLED")
    service_name: str = Field(default="kafka-async-service", alias="OTEL_SERVICE_NAME")
    exporter_otlp_endpoint: str = Field(
        default="http://localhost:4317",
        alias="OTEL_EXPORTER_OTLP_ENDPOINT",
    )
    exporter_otlp_headers: str = Field(default="", alias="OTEL_EXPORTER_OTLP_HEADERS")
    traces_sampler: str = Field(
        default="parentbased_traceidratio",
        alias="OTEL_TRACES_SAMPLER",
    )
    traces_sampler_arg: float = Field(default=1.0, alias="OTEL_TRACES_SAMPLER_ARG")


class Settings:
    def __init__(
        self,
        app: AppSettings | None = None,
        kafka: KafkaSettings | None = None,
        logging: LoggingSettings | None = None,
        otel: OpenTelemetrySettings | None = None,
    ) -> None:
        self.app = app or AppSettings()
        self.kafka = kafka or KafkaSettings()
        self.logging = logging or LoggingSettings()
        self.otel = otel or OpenTelemetrySettings()


@lru_cache
def get_settings() -> Settings:
    return Settings()
