"""Validated event models and immutable message-processing values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator


class BaseEventEnvelope(BaseModel):
    """The minimum contract needed to route an event on the shared topic."""

    event_type: str
    schema_version: int
    event_id: str | None = None
    source: str | None = None
    occurred_at: datetime | None = None

    model_config = ConfigDict(extra="allow")


class CalculationRequested(BaseModel):
    """Version one input contract for a requested calculation."""

    event_type: Literal["calculation.requested"]
    schema_version: Literal[1]
    event_id: str | None = None
    source: str | None = None
    request_id: str
    calc_id: str
    calc_process: str
    occurred_at: AwareDatetime | None = None

    model_config = ConfigDict(extra="allow")

    @field_validator("request_id", "calc_id", "calc_process")
    @classmethod
    def identifiers_must_not_be_blank(cls, value: str) -> str:
        """Reject whitespace-only identifiers without changing opaque values."""
        if not value.strip():
            raise ValueError("identifier must not be blank")
        return value


class CalculationResult(BaseModel):
    """Data returned by a calculation implementation."""

    data: dict[str, Any]

    model_config = ConfigDict(extra="forbid")


class EventError(BaseModel):
    """Sanitized error exposed in a failed calculation event."""

    code: str
    message: str
    retryable: bool = False

    model_config = ConfigDict(extra="forbid")


class _CalculationOutcome(BaseModel):
    event_id: str
    schema_version: Literal[1] = 1
    source: str
    request_id: str
    calc_id: str
    calc_process: str
    occurred_at: AwareDatetime

    model_config = ConfigDict(extra="forbid")


class CalculationCompleted(_CalculationOutcome):
    """Successful calculation output contract."""

    event_type: Literal["calculation.completed"] = "calculation.completed"
    status: Literal["ok"] = "ok"
    result: dict[str, Any]


class CalculationFailed(_CalculationOutcome):
    """Terminal calculation failure output contract."""

    event_type: Literal["calculation.failed"] = "calculation.failed"
    status: Literal["error"] = "error"
    error: EventError


class DeadLetterHeader(BaseModel):
    """A source Kafka header encoded so any byte value is JSON-safe."""

    name: str
    value_base64: str | None

    model_config = ConfigDict(extra="forbid")


class DeadLetterError(BaseModel):
    """Sanitized reason for placing an input record on the DLQ."""

    code: str
    message: str

    model_config = ConfigDict(extra="forbid")


class DeadLetterRecord(BaseModel):
    """JSON-safe copy of a Kafka record that could not be processed."""

    source_topic: str
    source_partition: int
    source_offset: int
    source_key_base64: str | None
    source_value_base64: str
    source_headers: tuple[DeadLetterHeader, ...] = ()
    error: DeadLetterError
    failed_at: AwareDatetime
    service: str

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True, slots=True)
class MessageContext:
    """Source Kafka metadata made available to message handlers."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    headers: tuple[tuple[str, bytes | None], ...]
    raw_value: bytes = b""


@dataclass(frozen=True, slots=True)
class OutgoingRecord:
    """A Kafka record requested by a message handler."""

    topic: str
    key: str | bytes | None
    value: BaseModel | Mapping[str, Any]
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True, slots=True)
class HandlerResult:
    """All records that must be acknowledged before committing an input."""

    records: tuple[OutgoingRecord, ...]
    outcome: str
