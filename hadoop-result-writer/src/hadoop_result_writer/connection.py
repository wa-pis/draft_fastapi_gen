from __future__ import annotations

from .config import Config
from .kerberos import Kerberos
from .storage import Hdfs
from .trino import TrinoClient, TrinoWriter
from .writer import ResultWriter


class Connection:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.kerberos = Kerberos(config)
        self.hdfs = Hdfs(self.kerberos)
        self.trino = TrinoClient(config, self.kerberos)
        if config.mode == "hdfs":
            self.writer = ResultWriter(
                self.hdfs,
                config.hdfs_root,
                target_file_bytes=config.target_file_bytes,
                scratch_dir=config.scratch_dir,
                readback_attempts=config.readback_attempts,
                readback_delay=config.readback_delay,
            )
        else:
            self.writer = TrinoWriter(config, self.trino)

    def __enter__(self) -> Connection:
        self.kerberos.__enter__()
        return self

    def diagnose(self) -> dict[str, str]:
        """Read-only HDFS probe; actual write rights are checked by test publication."""
        status = {}
        self.kerberos.ensure()
        status["kerberos"] = "ok"
        if self.config.mode == "hdfs":
            self.hdfs._run("-ls", "-d", self.config.hdfs_root)
            status["hdfs"] = "ok (read-only probe)"
        else:
            self.trino.query("SELECT 1")
            status["trino"] = "ok (read-only probe)"
        return status

    def __exit__(self, *exc: object) -> None:
        self.kerberos.close()
