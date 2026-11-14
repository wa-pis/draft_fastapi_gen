from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

type JSONValue = bool | int | Decimal | str | list[JSONValue] | dict[str, JSONValue] | None


@dataclass(frozen=True, slots=True)
class Receipt:
    upload_id: UUID
    sha256: str
    row_count: int
    byte_count: int
    committed_at: datetime
    reused: bool


@dataclass(frozen=True, slots=True)
class ResultRow:
    row_number: int
    data: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class _Page:
    upload_id: UUID
    row_count: int
    limit: int
    rows: tuple[ResultRow, ...]


@dataclass(frozen=True, slots=True)
class OffsetPage(_Page):
    offset: int


@dataclass(frozen=True, slots=True)
class CursorPage(_Page):
    after_row_number: int
    next_after_row_number: int | None
