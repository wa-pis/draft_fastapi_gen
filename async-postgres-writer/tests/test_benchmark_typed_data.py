import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from importlib import import_module
from ipaddress import IPv4Address, IPv4Network
from pathlib import Path
from random import Random
from uuid import uuid4

import pytest
from asyncpg import BitString, Range

from async_postgres_writer.config import Settings
from async_postgres_writer.stream import NDJSONStream
from async_postgres_writer.writer import Store

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
codec = import_module("benchmarks.typed_data")
typed_writer = import_module("benchmarks.typed_writer")


def test_typed_rows_are_repeatable_and_exactly_four_kib_including_lf():
    first, second = Random(42), Random(42)
    for number in range(1, 41):
        line = codec.typed_row_bytes(number, first)
        assert line == codec.typed_row_bytes(number, second)
        assert len(line) == 4096
        assert line.endswith(b"\n")
        row = json.loads(line)
        padding = row["description"] if row["description"] is not None else row["metrics"]["note"]
        assert len(padding) > 3000


def test_typed_record_converts_precision_and_postgresql_specific_types():
    upload_id = uuid4()
    line = codec.typed_row_bytes(1, Random(42)).decode()
    wire = json.loads(line)
    values = dict(zip(codec.TYPED_COLUMNS, codec.typed_record(upload_id, 1, line), strict=True))
    assert (values["upload_id"], values["row_number"]) == (upload_id, 1)
    assert values["amount"] == Decimal("1.37")
    assert values["calculated_at"].utcoffset() == timedelta(0)
    assert values["source_local_at"].tzinfo is None
    assert values["processing_time"] == timedelta(microseconds=1001)
    assert values["source_ip"] == IPv4Address("192.0.2.2")
    assert values["source_network"] == IPv4Network("192.0.2.0/24")
    assert values["validity"] == Range(date(2026, 1, 1), date(2026, 2, 1))
    assert json.loads(values["metrics"]) == wire["metrics"]
    assert values["fingerprint"] == bytes.fromhex(wire["fingerprint"])
    assert values["quality_flags"] == BitString("00000001")


def test_nullable_fields_and_wrong_stream_positions():
    upload_id = uuid4()
    line = codec.typed_row_bytes(20, Random(42)).decode()
    values = dict(zip(codec.TYPED_COLUMNS, codec.typed_record(upload_id, 20, line), strict=True))
    assert values["description"] is None
    assert values["source_ip"] is None
    assert values["fingerprint"] is None
    assert len(json.loads(values["metrics"])["note"]) > 3000
    with pytest.raises(ValueError, match="stream position"):
        codec.typed_record(upload_id, 21, line)

    unexpected = json.loads(line)
    unexpected["extra"] = True
    with pytest.raises(ValueError, match="Unexpected typed benchmark fields"):
        codec.typed_record(upload_id, 20, unexpected)


async def test_typed_copy_parses_once_while_jsonb_copy_keeps_original_text(monkeypatch):
    upload_id = uuid4()
    line = b" \t" + codec.typed_row_bytes(1, Random(42))

    def make_stream():
        async def body():
            yield line

        return NDJSONStream(
            body(), max_line_bytes=8192, max_upload_bytes=8192, read_timeout_seconds=1
        )

    class Connection:
        records = None

        async def copy_records_to_table(self, table, **kwargs):
            self.records = [record async for record in kwargs["records"]]

    settings = Settings(
        database_url="postgresql://localhost/test",
        api_token="integration-test-token-32-characters",
        upload_timeout_seconds=1,
    )
    actual_loads = json.loads
    parse_calls = 0

    def counting_loads(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return actual_loads(*args, **kwargs)

    monkeypatch.setattr(json, "loads", counting_loads)
    typed_connection = Connection()
    await typed_writer.TypedStore(settings, None)._copy_rows(
        typed_connection, upload_id, make_stream()
    )
    assert parse_calls == 1
    assert len(typed_connection.records) == 1
    assert typed_connection.records[0][:2] == (upload_id, 1)

    parse_calls = 0
    jsonb_connection = Connection()
    await Store(settings, None)._copy_rows(jsonb_connection, upload_id, make_stream())
    assert parse_calls == 1
    assert jsonb_connection.records == [(upload_id, 1, line[:-1].decode())]
