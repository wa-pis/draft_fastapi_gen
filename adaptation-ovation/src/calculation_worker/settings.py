from __future__ import annotations

import re
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AnyHttpUrl,
    Field,
    PositiveFloat,
    PositiveInt,
    SecretStr,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from calculation_worker.errors import ConfigurationError

NonBlankString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    service_name: NonBlankString = Field(default="calculation-worker", alias="SERVICE_NAME")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    kafka_bootstrap_servers: NonBlankString = Field(
        default="kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic: NonBlankString = Field(default="INTEGRATIONS", alias="KAFKA_TOPIC")
    kafka_group_id: NonBlankString = Field(default="calculation-worker-v1", alias="KAFKA_GROUP_ID")
    kafka_client_id: NonBlankString = Field(
        default="calculation-worker-local", alias="KAFKA_CLIENT_ID"
    )
    kafka_auto_offset_reset: Literal["earliest", "latest", "error"] = Field(
        default="earliest", alias="KAFKA_AUTO_OFFSET_RESET"
    )
    kafka_max_poll_interval_ms: PositiveInt = Field(
        default=300_000, alias="KAFKA_MAX_POLL_INTERVAL_MS"
    )
    kafka_session_timeout_ms: PositiveInt = Field(default=45_000, alias="KAFKA_SESSION_TIMEOUT_MS")
    kafka_delivery_timeout_ms: PositiveInt = Field(
        default=30_000, alias="KAFKA_DELIVERY_TIMEOUT_MS"
    )
    kafka_request_timeout_ms: PositiveInt = Field(default=10_000, alias="KAFKA_REQUEST_TIMEOUT_MS")
    kafka_poll_timeout_seconds: PositiveFloat = Field(
        default=1.0, alias="KAFKA_POLL_TIMEOUT_SECONDS"
    )
    kafka_publish_timeout_seconds: PositiveFloat = Field(
        default=35.0, alias="KAFKA_PUBLISH_TIMEOUT_SECONDS"
    )
    kafka_shutdown_flush_timeout_seconds: PositiveFloat = Field(
        default=5.0, alias="KAFKA_SHUTDOWN_FLUSH_TIMEOUT_SECONDS"
    )
    kafka_security_protocol: NonBlankString = Field(
        default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL"
    )
    kafka_sasl_mechanism: NonBlankString | None = Field(default=None, alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: NonBlankString | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: SecretStr | None = Field(
        default=None, alias="KAFKA_SASL_PASSWORD", repr=False
    )
    kafka_ssl_ca_location: NonBlankString | None = Field(
        default=None, alias="KAFKA_SSL_CA_LOCATION"
    )

    dbos_system_database_url: SecretStr = Field(alias="DBOS_SYSTEM_DATABASE_URL", repr=False)
    dbos_executor_id: NonBlankString | None = Field(default=None, alias="DBOS_EXECUTOR_ID")
    dbos_application_version: NonBlankString = Field(default="v1", alias="DBOS_APPLICATION_VERSION")
    dbos_system_schema: NonBlankString = Field(default="dbos", alias="DBOS_SYSTEM_SCHEMA")
    dbos_worker_concurrency: PositiveInt = Field(default=4, alias="DBOS_WORKER_CONCURRENCY")
    dbos_max_recovery_attempts: PositiveInt = Field(default=3, alias="DBOS_MAX_RECOVERY_ATTEMPTS")
    dbos_shutdown_grace_seconds: int = Field(default=30, ge=0, alias="DBOS_SHUTDOWN_GRACE_SECONDS")

    upstream_api_base_url: AnyHttpUrl | None = Field(default=None, alias="UPSTREAM_API_BASE_URL")
    upstream_api_path_template: str = Field(
        default="/calculations/{calc_id}", alias="UPSTREAM_API_PATH_TEMPLATE"
    )
    upstream_api_token: SecretStr | None = Field(
        default=None, alias="UPSTREAM_API_TOKEN", repr=False
    )
    upstream_connect_timeout_seconds: PositiveFloat = Field(
        default=5.0, alias="UPSTREAM_CONNECT_TIMEOUT_SECONDS"
    )
    upstream_read_timeout_seconds: PositiveFloat = Field(
        default=30.0, alias="UPSTREAM_READ_TIMEOUT_SECONDS"
    )
    upstream_write_timeout_seconds: PositiveFloat = Field(
        default=10.0, alias="UPSTREAM_WRITE_TIMEOUT_SECONDS"
    )
    upstream_pool_timeout_seconds: PositiveFloat = Field(
        default=5.0, alias="UPSTREAM_POOL_TIMEOUT_SECONDS"
    )
    upstream_max_connections: PositiveInt = Field(default=20, alias="UPSTREAM_MAX_CONNECTIONS")
    upstream_max_keepalive_connections: PositiveInt = Field(
        default=10, alias="UPSTREAM_MAX_KEEPALIVE_CONNECTIONS"
    )
    metrics_port: int = Field(default=8000, ge=1, le=65_535, alias="METRICS_PORT")

    def __init__(self, **values: Any) -> None:
        try:
            super().__init__(**values)
        except ValidationError:
            raise ConfigurationError("Service configuration is invalid") from None

    @field_validator(
        "kafka_sasl_mechanism",
        "kafka_sasl_username",
        "kafka_sasl_password",
        "kafka_ssl_ca_location",
        "dbos_executor_id",
        "upstream_api_base_url",
        "upstream_api_token",
        mode="before",
    )
    @classmethod
    def blank_optional_values_are_unset(cls, value: object) -> object:
        return None if value == "" else value

    @field_validator("upstream_api_token")
    @classmethod
    def token_must_not_be_blank(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not value.get_secret_value().strip():
            raise ValueError("UPSTREAM_API_TOKEN must not be blank")
        return value

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL is invalid")
        return normalized

    @model_validator(mode="after")
    def validate_runtime_budget_and_security(self) -> Self:
        if self.kafka_delivery_timeout_ms < self.kafka_request_timeout_ms:
            raise ValueError("KAFKA_DELIVERY_TIMEOUT_MS must be at least KAFKA_REQUEST_TIMEOUT_MS")
        if self.kafka_publish_timeout_seconds < self.kafka_delivery_timeout_ms / 1000:
            raise ValueError("KAFKA_PUBLISH_TIMEOUT_SECONDS must cover delivery timeout")

        sasl_values = (
            self.kafka_sasl_mechanism,
            self.kafka_sasl_username,
            self.kafka_sasl_password,
        )
        if self.kafka_security_protocol.upper().startswith("SASL") and not all(sasl_values):
            raise ValueError("SASL security protocol requires mechanism, username, and password")
        if any(sasl_values) and not all(sasl_values):
            raise ValueError("SASL mechanism, username, and password must be configured together")

        base_url = self.upstream_api_base_url
        if (base_url is None) != (self.upstream_api_token is None):
            raise ValueError("UPSTREAM_API_BASE_URL and UPSTREAM_API_TOKEN must be set together")
        if base_url is not None and (base_url.username or base_url.password):
            raise ValueError("UPSTREAM_API_BASE_URL must not contain credentials")
        if (
            not self.upstream_api_path_template.startswith("/")
            or self.upstream_api_path_template.count("{calc_id}") != 1
            or "://" in self.upstream_api_path_template
            or "?" in self.upstream_api_path_template
            or "#" in self.upstream_api_path_template
        ):
            raise ValueError(
                "UPSTREAM_API_PATH_TEMPLATE must be an absolute path with one {calc_id} placeholder"
            )
        if self.upstream_max_keepalive_connections > self.upstream_max_connections:
            raise ValueError("UPSTREAM_MAX_KEEPALIVE_CONNECTIONS cannot exceed max connections")
        database_url = self.dbos_system_database_url.get_secret_value()
        if not database_url.startswith(("postgresql://", "postgresql+psycopg://")):
            raise ValueError("DBOS_SYSTEM_DATABASE_URL must be a PostgreSQL URL")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.dbos_system_schema):
            raise ValueError("DBOS_SYSTEM_SCHEMA must be a valid PostgreSQL identifier")
        return self

    def require_worker_executor_id(self) -> str:
        if self.dbos_executor_id is None:
            raise ConfigurationError("DBOS_EXECUTOR_ID is required in worker mode")
        return self.dbos_executor_id

    def validate_worker_mode(self) -> None:
        self.require_worker_executor_id()
        self.require_upstream_config()

    def require_upstream_config(self) -> tuple[AnyHttpUrl, SecretStr]:
        if self.upstream_api_base_url is None or self.upstream_api_token is None:
            raise ConfigurationError(
                "UPSTREAM_API_BASE_URL and UPSTREAM_API_TOKEN are required in worker mode"
            )
        return self.upstream_api_base_url, self.upstream_api_token

    def kafka_security_config(self) -> dict[str, object]:
        config: dict[str, object] = {"security.protocol": self.kafka_security_protocol}
        if self.kafka_sasl_mechanism is not None:
            config["sasl.mechanism"] = self.kafka_sasl_mechanism
        if self.kafka_sasl_username is not None:
            config["sasl.username"] = self.kafka_sasl_username
        if self.kafka_sasl_password is not None:
            config["sasl.password"] = self.kafka_sasl_password.get_secret_value()
        if self.kafka_ssl_ca_location is not None:
            config["ssl.ca.location"] = self.kafka_ssl_ca_location
        return config

    def kafka_consumer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.kafka_group_id,
            "client.id": self.kafka_client_id,
            "auto.offset.reset": self.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": self.kafka_max_poll_interval_ms,
            "session.timeout.ms": self.kafka_session_timeout_ms,
            **self.kafka_security_config(),
        }

    def kafka_producer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "client.id": self.kafka_client_id,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": self.kafka_delivery_timeout_ms,
            "request.timeout.ms": self.kafka_request_timeout_ms,
            **self.kafka_security_config(),
        }
