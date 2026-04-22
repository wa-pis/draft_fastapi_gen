### Configuration

This folder contains configuration management for the service.

- `settings.py` defines a `Settings` class based on `pydantic-settings`.
- All runtime configuration (DB/Kafka/Redis/telemetry/etc.) is provided via environment variables or a `.env` file.

Code outside this folder should access configuration via `get_settings()` instead of reading environment variables directly.
