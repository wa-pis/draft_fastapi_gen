from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,127}", value):
        raise ValueError(
            "SQL identifiers must use lowercase letters, digits and underscores (1–128)"
        )
    return value


def result_identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value):
        raise ValueError("result_id must contain 1–128 letters, digits, underscores or hyphens")
    return value


def hdfs_uri(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "hdfs"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or "%" in value
        or not parsed.path.startswith("/")
        or parsed.path.rstrip("/") == ""
        or any(part in {".", ".."} for part in parsed.path.split("/"))
        or any(char.isspace() for char in value)
    ):
        raise ValueError(
            "Expected a non-root absolute hdfs:// URI without credentials or traversal"
        )
    return value.rstrip("/")


@dataclass(frozen=True)
class Config:
    principal: str
    keytab: str
    hdfs_root: str
    mode: str = "hdfs"
    trino_host: str = "localhost"
    trino_user: str = "writer"
    catalog: str = "hive"
    schema: str = "writer_demo"
    table: str = "result"
    trino_port: int = 8443
    trino_service: str = "HTTP"
    ca_bundle: str | None = None
    refresh_seconds: float = 300
    command_timeout: float = 120
    target_file_bytes: int = 256 * 1024 * 1024
    scratch_dir: str = "/tmp"
    cache_dir: str = "/tmp"
    readback_attempts: int = 5
    readback_delay: float = 1

    def __post_init__(self) -> None:
        if self.mode not in {"hdfs", "trino"}:
            raise ValueError("mode must be hdfs or trino")
        for name in ("catalog", "schema", "table"):
            identifier(getattr(self, name))
        hdfs_uri(self.hdfs_root)
        if self.principal.startswith("-") or not re.fullmatch(r"[^\s@]+@[^\s@]+", self.principal):
            raise ValueError("Expected a Kerberos principal including its realm")
        if not self.trino_host or any(c in self.trino_host for c in "/:@ \n\r"):
            raise ValueError("trino_host must be a hostname without scheme, port or credentials")
        if not self.trino_user or not re.fullmatch(r"[A-Za-z0-9_.@-]+", self.trino_user):
            raise ValueError("Invalid Trino user")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.trino_service):
            raise ValueError("Invalid Trino service name")
        if not 1 <= self.trino_port <= 65535:
            raise ValueError("Invalid Trino port")
        if min(self.refresh_seconds, self.command_timeout, self.target_file_bytes) <= 0:
            raise ValueError("Timeouts, refresh interval and file size must be positive")
        if self.readback_attempts < 1 or self.readback_delay < 0:
            raise ValueError("Invalid readback retry budget")
        if not all(
            math.isfinite(value)
            for value in (self.refresh_seconds, self.command_timeout, self.readback_delay)
        ):
            raise ValueError("Timeouts must be finite")
        for name in ("keytab", "scratch_dir", "cache_dir"):
            if not Path(getattr(self, name)).is_absolute():
                raise ValueError(f"{name} must be an absolute path")
        if self.ca_bundle and not Path(self.ca_bundle).is_absolute():
            raise ValueError("ca_bundle must be an absolute path")

    @classmethod
    def load(cls, path: str | Path) -> Config:
        with Path(path).open() as stream:
            return cls(**json.load(stream))
