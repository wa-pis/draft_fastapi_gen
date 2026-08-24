from __future__ import annotations

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
    kafka_dlq_topic: NonBlankString = Field(default="INTEGRATIONS.DLQ", alias="KAFKA_DLQ_TOPIC")
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

    upstream_api_base_url: AnyHttpUrl = Field(alias="UPSTREAM_API_BASE_URL")
    upstream_api_path_template: str = Field(
        default="/calculations/{calc_id}", alias="UPSTREAM_API_PATH_TEMPLATE"
    )
    upstream_api_token: SecretStr = Field(alias="UPSTREAM_API_TOKEN", repr=False)
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
    upstream_max_attempts: PositiveInt = Field(default=3, alias="UPSTREAM_MAX_ATTEMPTS")
    upstream_retry_min_wait_seconds: float = Field(
        default=0.5, ge=0, alias="UPSTREAM_RETRY_MIN_WAIT_SECONDS"
    )
    upstream_retry_max_wait_seconds: float = Field(
        default=5.0, ge=0, alias="UPSTREAM_RETRY_MAX_WAIT_SECONDS"
    )
    upstream_retryable_status_codes_csv: str = Field(
        default="429,500,502,503,504", alias="UPSTREAM_RETRYABLE_STATUS_CODES"
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
        mode="before",
    )
    @classmethod
    def blank_optional_values_are_unset(cls, value: object) -> object:
        return None if value == "" else value

    @field_validator("upstream_api_token")
    @classmethod
    def token_must_not_be_blank(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
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
        if self.kafka_topic == self.kafka_dlq_topic:
            raise ValueError("KAFKA_TOPIC and KAFKA_DLQ_TOPIC must differ")
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
        if base_url.username or base_url.password:
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
        if self.upstream_retry_min_wait_seconds > self.upstream_retry_max_wait_seconds:
            raise ValueError("UPSTREAM_RETRY_MIN_WAIT_SECONDS cannot exceed the maximum wait")
        if self.upstream_max_keepalive_connections > self.upstream_max_connections:
            raise ValueError("UPSTREAM_MAX_KEEPALIVE_CONNECTIONS cannot exceed max connections")

        _ = self.upstream_retryable_status_codes
        per_attempt_seconds = (
            self.upstream_connect_timeout_seconds
            + self.upstream_read_timeout_seconds
            + self.upstream_write_timeout_seconds
            + self.upstream_pool_timeout_seconds
        )
        retry_budget_seconds = (
            self.upstream_max_attempts * per_attempt_seconds
            + (self.upstream_max_attempts - 1) * self.upstream_retry_max_wait_seconds
        )
        if (
            retry_budget_seconds + self.kafka_publish_timeout_seconds
            >= self.kafka_max_poll_interval_ms / 1000
        ):
            raise ValueError(
                "HTTP retry and Kafka publish budgets must fit KAFKA_MAX_POLL_INTERVAL_MS"
            )
        return self

    @property
    def upstream_retryable_status_codes(self) -> frozenset[int]:
        try:
            codes = frozenset(
                int(value.strip())
                for value in self.upstream_retryable_status_codes_csv.split(",")
                if value.strip()
            )
        except ValueError as exc:
            raise ValueError("UPSTREAM_RETRYABLE_STATUS_CODES must contain integers") from exc
        if not codes or any(code < 100 or code > 599 for code in codes):
            raise ValueError("UPSTREAM_RETRYABLE_STATUS_CODES contains an invalid HTTP status")
        return codes

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
