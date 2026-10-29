from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

from .config import Config
from .errors import AuthenticationError


class Kerberos:
    """One private credential cache per context. KRB5_CONFIG is process configuration."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._directory: TemporaryDirectory | None = None
        self._failed = False
        self.cache: str | None = None

    def _run(self, *args: str) -> None:
        try:
            subprocess.run(
                args,
                env=self.env,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.config.command_timeout,
            )
        except (OSError, subprocess.SubprocessError):
            raise AuthenticationError("Kerberos credential acquisition/check failed") from None

    @property
    def env(self) -> dict[str, str]:
        if self.cache is None:
            raise AuthenticationError("Kerberos context is not open")
        return {**os.environ, "KRB5CCNAME": self.cache}

    def refresh(self) -> None:
        with self._lock:
            try:
                self._run(
                    "kinit",
                    "-l",
                    "1h",
                    "-kt",
                    self.config.keytab,
                    "-c",
                    self.cache or "",
                    self.config.principal,
                )
                self._run("klist", "-s", "-c", self.cache or "")
                os.chmod((self.cache or "").removeprefix("FILE:"), 0o600)
            except (AuthenticationError, OSError):
                self._failed = True
                raise AuthenticationError("Unable to obtain valid Kerberos credentials") from None
            self._failed = False

    def ensure(self) -> None:
        with self._lock:
            if self._directory is None or self._failed:
                raise AuthenticationError("Kerberos credentials are unavailable; retry connection")
            try:
                self._run("klist", "-s", "-c", self.cache or "")
            except AuthenticationError:
                self.refresh()

    def _maintain(self) -> None:
        while not self._stop.wait(self.config.refresh_seconds):
            try:
                self.refresh()
            except AuthenticationError:
                # Retry at the next interval, but fail closed for callers meanwhile.
                pass

    def __enter__(self) -> Kerberos:
        if self._directory is not None:
            raise AuthenticationError("Kerberos context is already open")
        self._stop.clear()
        self._directory = TemporaryDirectory(prefix="hrw-krb-", dir=self.config.cache_dir)
        self.cache = "FILE:" + str(Path(self._directory.name) / "ccache")
        try:
            self.refresh()
        except BaseException:
            self.close()
            raise
        self._thread = threading.Thread(target=self._maintain, name="kerberos-refresh", daemon=True)
        self._thread.start()
        return self

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        if self._directory is not None:
            self._directory.cleanup()
            self._directory = None
        self.cache = None

    def __exit__(self, *exc: object) -> None:
        self.close()
