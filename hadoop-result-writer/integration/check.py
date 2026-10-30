from __future__ import annotations

import json
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow as pa

from hadoop_result_writer import Config, Connection
from hadoop_result_writer.cli import synthetic_batches
from hadoop_result_writer.errors import Conflict, StorageError

config = Config(
    principal="writer@WRITER.TEST",
    keytab="/tmp/hrw-cluster/writer.keytab",
    hdfs_root="hdfs://hadoop:9000/writer",
    refresh_seconds=5,
    target_file_bytes=8 * 1024 * 1024,
)
with Connection(config) as connection:
    assert connection.diagnose()["kerberos"] == "ok"
    assert connection.writer.lookup("missing-before-first-publication") is None
    dataframe = pd.DataFrame({"value": [1, None, 3]})
    assert (
        connection.writer.publish(
            "pandas-hdfs", dataframe, schema=pa.schema([("value", pa.int64())]), batch_size=2
        ).rows
        == 3
    )
    receipt = connection.writer.publish("real-hdfs", synthetic_batches(16 * 1024 * 1024))
    assert receipt.rows == 16384 and receipt.files == 2
    assert connection.writer.lookup("real-hdfs").manifest_sha256 == receipt.manifest_sha256
    assert connection.writer.publish("real-hdfs", synthetic_batches(16 * 1024 * 1024)).reused
    try:
        connection.writer.publish("real-hdfs", synthetic_batches(8 * 1024 * 1024))
        raise AssertionError("Expected conflict")
    except Conflict:
        pass
    # Explicitly verify Hadoop mv cannot replace a previously published commit file.
    with TemporaryDirectory() as directory:
        path = Path(directory) / "candidate.json"
        path.write_text('{"bad": true}')
        candidate = config.hdfs_root + "/candidate.json"
        connection.hdfs.put(path, candidate)
        try:
            connection.hdfs.rename(candidate, config.hdfs_root + "/_commits/real-hdfs.json")
            raise AssertionError("HDFS mv overwrote a commit file")
        except StorageError:
            pass
    assert connection.writer.lookup("real-hdfs").manifest_sha256 == receipt.manifest_sha256
    time.sleep(6)
    connection.kerberos.ensure()
    # Missing credentials must not silently fall back to simple authentication.
    rejected = subprocess.run(
        ["hdfs", "dfs", "-ls", config.hdfs_root],
        env={**connection.kerberos.env, "KRB5CCNAME": "FILE:/tmp/does-not-exist"},
        capture_output=True,
        timeout=60,
    )
    assert rejected.returncode != 0
    print(json.dumps({"secure_hdfs": "passed", "refresh": "passed", **asdict(receipt)}))
