from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .config import hdfs_uri, result_identifier
from .errors import Conflict, PublicationUnknown, StorageError, WriterError
from .input import as_batches
from .storage import FileSystem


@dataclass(frozen=True)
class Receipt:
    result_id: str
    location: str
    rows: int
    files: int
    manifest_sha256: str
    reused: bool = False


class ResultWriter:
    def __init__(
        self,
        filesystem: FileSystem,
        root: str,
        *,
        target_file_bytes: int = 256 * 1024 * 1024,
        scratch_dir: str = "/tmp",
        readback_attempts: int = 5,
        readback_delay: float = 1,
    ) -> None:
        if target_file_bytes < 1 or readback_attempts < 1 or readback_delay < 0:
            raise ValueError("Invalid file size or readback budget")
        self.fs = filesystem
        self.root = hdfs_uri(root)
        self.target_file_bytes = target_file_bytes
        self.scratch_dir = scratch_dir
        self.readback_attempts = readback_attempts
        self.readback_delay = readback_delay

    def lookup(self, result_id: str) -> Receipt | None:
        result_identifier(result_id)
        try:
            if not self.fs.exists(self.root + "/_commits"):
                return None
            commit_path = f"{self.root}/_commits/{result_id}.json"
            if not self.fs.exists(commit_path):
                return None
            commit = json.loads(self.fs.read(commit_path))
            location = commit["location"]
            prefix = self.root + "/results/"
            if not location.startswith(prefix):
                raise ValueError("Unexpected result location")
            UUID(location.removeprefix(prefix))
            raw = self.fs.read(location + "/_manifest.json")
            if hashlib.sha256(raw).hexdigest() != commit["manifest_sha256"]:
                raise ValueError("Manifest hash mismatch")
            manifest = json.loads(raw)
            if manifest["version"] != 1 or manifest["result_id"] != result_id:
                raise ValueError("Unexpected manifest identity")
            files = manifest["files"]
            if not files or len(files) != len({item["name"] for item in files}):
                raise ValueError("Empty or duplicate manifest entries")
            for item in files:
                if not re.fullmatch(r"part-[0-9]{6,}\.parquet", item["name"]):
                    raise ValueError("Unexpected filename")
                if (
                    not isinstance(item["rows"], int)
                    or item["rows"] < 1
                    or not isinstance(item["bytes"], int)
                    or item["bytes"] < 1
                    or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
                ):
                    raise ValueError("Invalid manifest file metadata")
                if item["bytes"] != self.fs.size(location + "/" + item["name"]):
                    raise ValueError("File size mismatch")
            expected_rows = {item["name"]: item["rows"] for item in files}
            return Receipt(
                result_id,
                location,
                sum(expected_rows.values()),
                len(files),
                hashlib.sha256(raw).hexdigest(),
                reused=True,
            )
        except (StorageError, OSError, ValueError, KeyError, TypeError):
            raise PublicationUnknown(
                "Cannot confirm publication; preserve data and retry lookup"
            ) from None

    def _confirm(self, result_id: str) -> Receipt:
        for attempt in range(self.readback_attempts):
            try:
                receipt = self.lookup(result_id)
                if receipt is not None:
                    return receipt
            except PublicationUnknown:
                pass
            if attempt + 1 < self.readback_attempts:
                time.sleep(self.readback_delay)
        raise PublicationUnknown("HDFS commit outcome is unknown; retry lookup, do not delete data")

    def publish(
        self,
        result_id: str,
        batches: Any,
        *,
        schema: pa.Schema | None = None,
        batch_size: int = 8192,
    ) -> Receipt:
        batches = as_batches(batches, schema=schema, batch_size=batch_size)
        result_identifier(result_id)
        attempt = str(uuid4())
        staging = f"{self.root}/staging/{attempt}"
        final = f"{self.root}/results/{attempt}"
        self.fs.mkdir(staging)
        self.fs.mkdir(self.root + "/results")
        self.fs.mkdir(self.root + "/_commits")
        schema = None
        entries: list[dict] = []
        with TemporaryDirectory(prefix="hrw-data-", dir=self.scratch_dir) as directory:
            path = Path(directory) / "current.parquet"
            writer = None
            file_rows = 0
            file_bytes = 0

            def finish_file() -> None:
                nonlocal writer, file_rows, file_bytes
                if writer is None:
                    return
                writer.close()
                writer = None
                name = f"part-{len(entries):06d}.parquet"
                with path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                size = path.stat().st_size
                self.fs.put(path, staging + "/" + name)
                if self.fs.size(staging + "/" + name) != size:
                    raise StorageError("Uploaded file size mismatch")
                entries.append({"name": name, "bytes": size, "rows": file_rows, "sha256": digest})
                path.unlink()
                file_rows = file_bytes = 0

            try:
                for batch in batches:
                    if not isinstance(batch, pa.RecordBatch):
                        raise ValueError("Input must contain Arrow RecordBatch objects")
                    if schema is None:
                        schema = batch.schema
                        if not schema.names or len(set(schema.names)) != len(schema):
                            raise ValueError("Empty or duplicate schema columns")
                    if not schema.equals(batch.schema, check_metadata=True):
                        raise ValueError("Input schema changed during publication")
                    if batch.num_rows == 0:
                        continue
                    if writer is None:
                        writer = pq.ParquetWriter(path, schema, compression="zstd")
                    writer.write_batch(batch)
                    file_rows += batch.num_rows
                    file_bytes += batch.nbytes
                    # Rotate by uncompressed bytes; compressed file size is data-dependent.
                    if file_bytes >= self.target_file_bytes:
                        finish_file()
                finish_file()
            finally:
                if writer is not None:
                    writer.close()
            if not entries or schema is None:
                raise ValueError("Empty results are not supported by this prototype")
            manifest = {
                "version": 1,
                "result_id": result_id,
                "schema_sha256": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
                "files": entries,
            }
            raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            if len(raw) > 16 * 1024 * 1024:
                raise ValueError("Too many files for a prototype manifest")
            manifest_path = Path(directory) / "_manifest.json"
            manifest_path.write_bytes(raw)
            self.fs.put(manifest_path, staging + "/_manifest.json")
            if self.fs.size(staging + "/_manifest.json") != len(raw):
                raise StorageError("Uploaded manifest size mismatch")
            commit_path = Path(directory) / "commit.json"
            commit_path.write_text(
                json.dumps(
                    {"location": final, "manifest_sha256": hashlib.sha256(raw).hexdigest()},
                    sort_keys=True,
                )
            )
            self.fs.put(commit_path, staging + "/_commit.json")

        digest = hashlib.sha256(raw).hexdigest()
        existing = self.lookup(result_id)
        if existing is not None:
            if existing.manifest_sha256 != digest:
                raise Conflict("result_id is already published with a different manifest")
            return existing
        self.fs.rename(staging, final)
        try:
            self.fs.rename(final + "/_commit.json", f"{self.root}/_commits/{result_id}.json")
        except StorageError:
            # A lost rename response and a competing commit are resolved by readback.
            pass
        confirmed = self._confirm(result_id)
        if confirmed.manifest_sha256 != digest:
            raise Conflict("A different result won concurrent HDFS commit")
        return Receipt(**{**asdict(confirmed), "reused": confirmed.location != final})


def safe_error(error: Exception) -> str:
    if isinstance(error, WriterError):
        return str(error)
    return "Operation failed; check input, configuration and connectivity"
