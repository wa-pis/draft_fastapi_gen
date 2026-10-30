"""Local filesystem demonstration; not a simulation of HDFS replication or authentication."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit

from .errors import StorageError


class LocalFileSystem:
    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.peak_scratch_bytes = 0

    def path(self, uri: str) -> Path:
        parsed = urlsplit(uri)
        if parsed.scheme != "hdfs" or parsed.netloc != "local":
            raise StorageError("Local demo only accepts hdfs://local URIs")
        path = (self.directory / parsed.path.lstrip("/")).resolve()
        if self.directory not in path.parents:
            raise StorageError("Path escapes local demo directory")
        return path

    def mkdir(self, uri: str) -> None:
        self.path(uri).mkdir(parents=True, exist_ok=True)

    def put(self, local: Path, uri: str) -> None:
        self.peak_scratch_bytes = max(self.peak_scratch_bytes, local.stat().st_size)
        with local.open("rb") as source, self.path(uri).open("xb") as target:
            shutil.copyfileobj(source, target)

    def read(self, uri: str) -> bytes:
        if self.size(uri) > 16 * 1024 * 1024:
            raise StorageError("Manifest exceeds 16 MiB")
        return self.path(uri).read_bytes()

    def size(self, uri: str) -> int:
        return self.path(uri).stat().st_size

    def exists(self, uri: str) -> bool:
        return self.path(uri).exists()

    def rename(self, source: str, destination: str) -> None:
        source_path, destination_path = self.path(source), self.path(destination)
        if source_path.is_dir():
            source_path.rename(destination_path)
        else:
            try:
                os.link(source_path, destination_path)
                source_path.unlink()
            except FileExistsError:
                raise StorageError("Commit already exists") from None
