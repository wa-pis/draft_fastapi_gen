from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Protocol

from .errors import StorageError
from .kerberos import Kerberos


class FileSystem(Protocol):
    def mkdir(self, uri: str) -> None: ...
    def put(self, local: Path, uri: str) -> None: ...
    def read(self, uri: str) -> bytes: ...
    def size(self, uri: str) -> int: ...
    def rename(self, source: str, destination: str) -> None: ...
    def exists(self, uri: str) -> bool: ...


class Hdfs:
    def __init__(self, kerberos: Kerberos) -> None:
        self.kerberos = kerberos

    def _run(self, *args: str, capture: bool = False) -> bytes:
        self.kerberos.ensure()
        try:
            result = subprocess.run(
                ["hdfs", "dfs", *args],
                env=self.kerberos.env,
                stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=self.kerberos.config.command_timeout,
            )
        except (OSError, subprocess.SubprocessError):
            raise StorageError("HDFS operation failed; prepared data has been preserved") from None
        return result.stdout or b""

    def mkdir(self, uri: str) -> None:
        self._run("-mkdir", "-p", uri)

    def put(self, local: Path, uri: str) -> None:
        self._run("-put", str(local), uri)

    def read(self, uri: str) -> bytes:
        # Only small manifests are read into memory, never Parquet data.
        if self.size(uri) > 16 * 1024 * 1024:
            raise StorageError("Manifest exceeds the 16 MiB limit")
        return self._run("-cat", uri, capture=True)

    def size(self, uri: str) -> int:
        try:
            return int(self._run("-stat", "%b", uri, capture=True).strip())
        except ValueError:
            raise StorageError("Invalid HDFS file size response") from None

    def rename(self, source: str, destination: str) -> None:
        # Commit destinations are files: HDFS mv must never overwrite them.
        self._run("-mv", source, destination)

    def exists(self, uri: str) -> bool:
        parent, name = uri.rsplit("/", 1)
        # Successful listing distinguishes absence from an authentication/network failure.
        lines = self._run("-ls", parent, capture=True).decode("utf-8").splitlines()
        return any(
            len(parts := line.split(None, 7)) == 8 and parts[7].rsplit("/", 1)[-1] == name
            for line in lines
        )
