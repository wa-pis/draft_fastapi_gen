import json
from datetime import date, timedelta
from decimal import Decimal
from importlib.util import module_from_spec, spec_from_file_location
from ipaddress import IPv4Address, IPv4Network
from pathlib import Path
from random import Random
from uuid import uuid4

import pytest
from asyncpg import BitString, Range

spec = spec_from_file_location(
    "typed_data", Path(__file__).parents[1] / "benchmarks" / "typed_data.py"
)
codec = module_from_spec(spec)
spec.loader.exec_module(codec)


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
