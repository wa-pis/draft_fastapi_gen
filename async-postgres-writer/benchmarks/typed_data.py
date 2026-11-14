"""Deterministic NDJSON and binary COPY records for the typed-table benchmark."""

import base64
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal, DecimalException
from ipaddress import ip_address, ip_network
from random import Random
from uuid import UUID

from asyncpg import BitString, Range

ROW_BYTES = 4096
TYPED_COLUMNS = (
    "upload_id",
    "row_number",
    "scenario_id",
    "model_version",
    "account_code",
    "currency_code",
    "description",
    "is_final",
    "amount",
    "confidence",
    "score",
    "calculation_date",
    "calculated_at",
    "source_local_at",
    "cutoff_time",
    "processing_time",
    "source_ip",
    "source_network",
    "labels",
    "sample_ids",
    "validity",
    "metrics",
    "fingerprint",
    "confidence_band",
    "quality_flags",
)
_WIRE_FIELDS = frozenset(TYPED_COLUMNS[1:])
_FILLER = "@payload@"
_FILLER_BYTES = _FILLER.encode("ascii")


def typed_row_bytes(number: int, generator: Random) -> bytes:
    """Return one 4096-byte NDJSON row (including LF), with no upload-specific data."""
    day = (number - 1) % 28 + 1
    calculation_date = f"2026-01-{day:02d}"
    cents = number * 137 % 50_000_000
    optional = number % 5 != 0
    has_description = number % 20 != 0
    row = {
        "row_number": number,
        "scenario_id": 100 + number % 64,
        "model_version": 1 + number % 8,
        "account_code": f"ACCT-{number % 100_000:05d}",
        "currency_code": ("USD", "EUR", "GBP")[number % 3],
        "description": _FILLER if has_description else None,
        "is_final": number % 3 != 0,
        "amount": f"{cents // 100}.{cents % 100:02d}",
        "confidence": 0.75 + (number % 4) / 16,
        "score": (number % 8000) / 8,
        "calculation_date": calculation_date,
        "calculated_at": f"{calculation_date}T12:00:{number % 60:02d}+00:00",
        "source_local_at": f"{calculation_date}T15:00:00" if optional else None,
        "cutoff_time": "17:30:00" if optional else None,
        "processing_time": 1000 + number % 100_000,  # Integer microseconds on the wire.
        "source_ip": f"192.0.2.{number % 254 + 1}" if optional else None,
        "source_network": "192.0.2.0/24" if optional else None,
        "labels": ["calculation", f"batch-{number % 16}"],
        "sample_ids": [number * 2, number * 2 + 1],
        "validity": [calculation_date, f"2026-02-{day:02d}"],
        "metrics": {"observations": number % 1000} if optional else None,
        "fingerprint": generator.randbytes(16).hex() if optional else None,
        "confidence_band": ["0.70", "1.00"] if optional else None,
        "quality_flags": f"{number % 256:08b}",
    }
    if not has_description:
        row["metrics"] = {"observations": number % 1000, "note": _FILLER}

    template = json.dumps(row, separators=(",", ":")).encode("ascii")
    length = ROW_BYTES - len(template) + len(_FILLER_BYTES) - 1
    if length < 0:
        raise ValueError("Typed benchmark fields exceed the NDJSON row size")
    # Base64 can be inserted into the quoted field without any JSON escapes.
    payload = base64.b64encode(generator.randbytes((length + 3) // 4 * 3))[:length]
    return template.replace(_FILLER_BYTES, payload, 1) + b"\n"


def _optional(value, convert):
    return None if value is None else convert(value)


def _range(value, convert):
    lower, upper = value
    return Range(convert(lower), convert(upper))


def typed_record(upload_id: UUID, row_number: int, row: dict[str, object] | str) -> tuple:
    """Convert a decoded row (or seed text during readback) for asyncpg COPY."""
    if isinstance(row, str):
        row = json.loads(row, parse_float=Decimal)
    if not isinstance(row, dict) or row.keys() != _WIRE_FIELDS:
        raise ValueError("Unexpected typed benchmark fields")
    if type(row["row_number"]) is not int or row["row_number"] != row_number:
        raise ValueError("Typed benchmark row_number does not match its stream position")

    try:
        calculated_at = datetime.fromisoformat(row["calculated_at"])
        local_at = _optional(row["source_local_at"], datetime.fromisoformat)
        cutoff = _optional(row["cutoff_time"], time.fromisoformat)
        if calculated_at.tzinfo is None or any(
            value is not None and value.tzinfo is not None for value in (local_at, cutoff)
        ):
            raise ValueError("Unexpected timezone in typed benchmark row")
        flags = row["quality_flags"]
        if not isinstance(flags, str) or len(flags) != 8 or flags.strip("01"):
            raise ValueError("quality_flags must contain eight binary digits")
        confidence, score = row["confidence"], row["score"]
        metrics = row["metrics"]
        if metrics is not None:
            if not isinstance(metrics, dict):
                raise ValueError("metrics must be a JSON object")
            # Keep the benchmark's existing float precision for metrics JSON.
            metrics = json.dumps(metrics, separators=(",", ":"), allow_nan=False, default=float)

        return (
            upload_id,
            row_number,
            row["scenario_id"],
            row["model_version"],
            row["account_code"],
            row["currency_code"],
            row["description"],
            row["is_final"],
            Decimal(row["amount"]),
            float(confidence) if isinstance(confidence, Decimal) else confidence,
            float(score) if isinstance(score, Decimal) else score,
            date.fromisoformat(row["calculation_date"]),
            calculated_at,
            local_at,
            cutoff,
            timedelta(microseconds=row["processing_time"]),
            _optional(row["source_ip"], ip_address),
            _optional(row["source_network"], ip_network),
            row["labels"],
            row["sample_ids"],
            _range(row["validity"], date.fromisoformat),
            metrics,
            _optional(row["fingerprint"], bytes.fromhex),
            None if row["confidence_band"] is None else _range(row["confidence_band"], Decimal),
            BitString(flags),
        )
    except (TypeError, OverflowError, DecimalException) as exc:
        raise ValueError("Invalid value in typed benchmark row") from exc
