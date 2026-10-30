from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hadoop_result_writer.errors import Conflict, PublicationUnknown, StorageError
from hadoop_result_writer.local import LocalFileSystem
from hadoop_result_writer.writer import ResultWriter


def batches(value=0):
    for start in range(0, 24, 4):
        yield pa.record_batch(
            [pa.array(range(start + value, start + value + 4), type=pa.int64())], names=["value"]
        )


@pytest.fixture
def setup(tmp_path):
    fs = LocalFileSystem(tmp_path)
    writer = ResultWriter(
        fs,
        "hdfs://local/writer",
        target_file_bytes=64,
        scratch_dir=str(tmp_path),
        readback_attempts=2,
        readback_delay=0,
    )
    return fs, writer


def test_publish_and_reuse(setup):
    fs, writer = setup
    receipt = writer.publish("calc-1", batches())
    assert (receipt.rows, receipt.files, receipt.reused) == (24, 3, False)
    assert writer.publish("calc-1", batches()).reused
    assert writer.lookup("calc-1").reused
    assert (
        sum(
            pq.ParquetFile(p).metadata.num_rows for p in fs.path(receipt.location).glob("*.parquet")
        )
        == 24
    )
    assert not list(fs.directory.glob("hrw-data-*"))


def test_publish_dataframe_and_python_records(setup):
    _, writer = setup
    schema = pa.schema([("value", pa.int64())])
    assert writer.publish("dataframe", pd.DataFrame({"value": [1, 2]}), schema=schema).rows == 2
    assert writer.publish("records", ({"value": v} for v in range(3)), schema=schema).rows == 3


def test_conflict_never_overwrites(setup):
    _, writer = setup
    original = writer.publish("calc-1", batches())
    with pytest.raises(Conflict):
        writer.publish("calc-1", batches(100))
    assert writer.lookup("calc-1").location == original.location


@pytest.mark.parametrize("point", ["put", "rename"])
def test_preparation_failure_does_not_commit(setup, monkeypatch, point):
    fs, writer = setup

    def fail(*args):
        raise StorageError("injected")

    monkeypatch.setattr(fs, point, fail)
    with pytest.raises(StorageError):
        writer.publish("calc-1", batches())
    assert writer.lookup("calc-1") is None


def test_lost_commit_response_is_recovered(setup, monkeypatch):
    fs, writer = setup
    rename = fs.rename

    def lost_response(source, destination):
        rename(source, destination)
        if "/_commits/" in destination:
            raise StorageError("response lost")

    monkeypatch.setattr(fs, "rename", lost_response)
    assert writer.publish("calc-1", batches()).rows == 24


def test_unknown_commit_preserves_files_and_can_retry(setup, monkeypatch):
    fs, writer = setup
    rename = fs.rename

    def fail_commit(source, destination):
        if "/_commits/" in destination:
            raise StorageError("failed")
        rename(source, destination)

    monkeypatch.setattr(fs, "rename", fail_commit)
    with pytest.raises(PublicationUnknown):
        writer.publish("calc-1", batches())
    assert len(list(fs.path("hdfs://local/writer/results").iterdir())) == 1
    monkeypatch.setattr(fs, "rename", rename)
    assert writer.publish("calc-1", batches()).rows == 24


def test_unknown_is_not_missing(setup, monkeypatch):
    fs, writer = setup

    def offline(*args):
        raise StorageError("offline")

    monkeypatch.setattr(fs, "exists", offline)
    with pytest.raises(PublicationUnknown):
        writer.lookup("calc-1")


@pytest.mark.parametrize("corruption", ["size", "manifest", "missing"])
def test_corruption_is_not_success(setup, corruption):
    fs, writer = setup
    receipt = writer.publish("calc-1", batches())
    manifest = fs.path(receipt.location + "/_manifest.json")
    content = json.loads(manifest.read_bytes())
    if corruption == "missing":
        manifest.unlink()
    elif corruption == "manifest":
        content["result_id"] = "wrong"
        manifest.write_text(json.dumps(content))
    else:
        file = fs.path(receipt.location + "/" + content["files"][0]["name"])
        with file.open("ab") as stream:
            stream.write(b"extra")
    with pytest.raises(PublicationUnknown):
        writer.lookup("calc-1")


def test_concurrent_attempts_have_one_winner(setup, monkeypatch):
    fs, writer = setup
    barrier = Barrier(2)
    rename = fs.rename

    def concurrent(source, destination):
        if "/_commits/" in destination:
            barrier.wait(timeout=5)
        rename(source, destination)

    monkeypatch.setattr(fs, "rename", concurrent)

    def publish(value):
        try:
            return writer.publish("shared", batches(value))
        except Conflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish, [0, 100]))
    assert sum(item is not None for item in results) == 1
    assert writer.lookup("shared").rows == 24


def test_empty_and_schema_change_fail_without_commit(setup):
    _, writer = setup
    with pytest.raises(ValueError, match="Empty"):
        writer.publish("empty", [])
    with pytest.raises(ValueError, match="schema changed"):
        writer.publish("changed", [next(batches()), pa.record_batch([["s"]], names=["value"])])
    assert writer.lookup("changed") is None


def test_incremental_upload(setup):
    fs, writer = setup

    def source():
        for index, batch in enumerate(batches()):
            if index >= 2:
                assert list(fs.path("hdfs://local/writer/staging").glob("*/*.parquet"))
            yield batch

    assert writer.publish("incremental", source()).rows == 24


def test_direct_hdfs_accepts_result_id_data_column(setup):
    _, writer = setup
    assert writer.publish("opaque", [pa.record_batch([["payload"]], names=["result_id"])]).rows == 1
