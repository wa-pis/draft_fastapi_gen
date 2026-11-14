from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WRITER_", hide_input_in_errors=True)

    database_url: SecretStr
    api_token: SecretStr
    pool_size: int = Field(default=4, ge=1, le=32)
    max_upload_bytes: int = Field(default=5 * 1024**3, ge=1)
    max_line_bytes: int = Field(default=1024**2, ge=1)
    upload_timeout_seconds: float = Field(default=3600, gt=0, allow_inf_nan=False)
    export_timeout_seconds: float = Field(default=3600, gt=0, allow_inf_nan=False)
    read_timeout_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    acquire_timeout_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    db_command_timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)

    @field_validator("database_url")
    @classmethod
    def postgres_url(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(("postgresql://", "postgres://")):
            raise ValueError("database_url must be a PostgreSQL connection URI")
        return value

    @field_validator("api_token")
    @classmethod
    def bearer_token(cls, value: SecretStr) -> SecretStr:
        token = value.get_secret_value()
        if len(token) < 32 or not token.isascii() or any(char.isspace() for char in token):
            raise ValueError("api_token must contain at least 32 ASCII characters without spaces")
        return value
