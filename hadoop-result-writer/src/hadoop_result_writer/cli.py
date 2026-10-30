from __future__ import annotations

import argparse
import json
import random
import resource
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .connection import Connection
from .local import LocalFileSystem
from .writer import ResultWriter, safe_error


def synthetic_batches(total_bytes: int, batch_bytes: int = 8 * 1024 * 1024):
    """Deterministic incompressible payloads, generated one bounded batch at a time."""
    random_source = random.Random(42)
    rows = max(1, batch_bytes // 1024)
    remaining = max(1, (total_bytes + 1023) // 1024)
    offset = 0
    while remaining:
        count = min(remaining, rows)
        yield pa.record_batch(
            [
                pa.array(range(offset, offset + count), type=pa.int64()),
                pa.array([random_source.randbytes(1016) for _ in range(count)], type=pa.binary()),
            ],
            names=["row_id", "payload"],
        )
        remaining -= count
        offset += count


def main() -> int:
    parser = argparse.ArgumentParser(description="Standalone HDFS / Trino writer prototype")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("diagnose", "lookup", "publish", "sample"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True, type=Path)
        if name != "diagnose":
            command.add_argument("--result-id", required=True)
        if name == "publish":
            command.add_argument("--input", required=True, type=Path, help="A local Parquet file")
        if name == "sample":
            command.add_argument("--mib", type=int, default=32)
    demo = commands.add_parser(
        "demo", help="Local filesystem; no Hadoop authentication/replication"
    )
    demo.add_argument("--directory", type=Path, required=True)
    demo.add_argument("--mib", type=int, default=32)
    demo.add_argument("--result-id", default="demo-1")
    args = parser.parse_args()
    if hasattr(args, "mib") and args.mib < 1:
        parser.error("--mib must be positive")
    started = time.monotonic()
    try:
        if args.command == "demo":
            args.directory.mkdir(parents=True, exist_ok=True)
            fs = LocalFileSystem(args.directory)
            writer = ResultWriter(
                fs,
                "hdfs://local/writer",
                target_file_bytes=16 * 1024 * 1024,
                scratch_dir=str(args.directory),
            )
            receipt = writer.publish(args.result_id, synthetic_batches(args.mib * 1024 * 1024))
            result = {
                **asdict(receipt),
                "mode": "local-demo",
                "input_mib": args.mib,
                "peak_local_file_bytes": fs.peak_scratch_bytes,
            }
        else:
            with Connection(Config.load(args.config)) as connection:
                if args.command == "diagnose":
                    result = connection.diagnose()
                elif args.command == "lookup":
                    receipt = connection.writer.lookup(args.result_id)
                    result = asdict(receipt) if receipt else {"state": "not_committed"}
                else:
                    if args.command == "sample":
                        batches = synthetic_batches(args.mib * 1024 * 1024)
                    else:
                        batches = pq.ParquetFile(args.input).iter_batches(batch_size=8192)
                    result = asdict(connection.writer.publish(args.result_id, batches))
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["peak_rss_bytes"] = rss if sys.platform == "darwin" else rss * 1024
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        print(
            json.dumps({"error": type(error).__name__, "message": safe_error(error)}),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
