from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, replace
from datetime import date
from typing import Any
from uuid import UUID, uuid4

import pyarrow as pa

from .config import Config, identifier, result_identifier
from .errors import CatalogError, Conflict, PublicationUnknown
from .input import as_batches
from .kerberos import Kerberos


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    if isinstance(value, str):
        return literal(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and math.isfinite(value):
        return repr(value)
    if isinstance(value, date):
        return "DATE " + literal(value.isoformat())
    raise ValueError("Unsupported SQL value (including non-finite floats)")


class TrinoClient:
    def __init__(self, config: Config, kerberos: Kerberos) -> None:
        self.config = config
        self.kerberos = kerberos

    def query(self, sql: str) -> list[list[Any]]:
        import gssapi
        import requests_gssapi
        from trino.auth import Authentication
        from trino.dbapi import connect

        self.kerberos.ensure()
        cache = self.kerberos.cache
        service, host = self.config.trino_service, self.config.trino_host

        class CacheAuthentication(Authentication):
            def set_http_session(self, session: Any) -> Any:
                session.trust_env = False
                session.auth = requests_gssapi.HTTPSPNEGOAuth(
                    creds=gssapi.Credentials(store={"ccache": cache}, usage="initiate"),
                    target_name=gssapi.Name(
                        f"{service}@{host}", name_type=gssapi.NameType.hostbased_service
                    ),
                    mutual_authentication=requests_gssapi.REQUIRED,
                    opportunistic_auth=True,
                )
                return session

            def get_exceptions(self) -> tuple[type[Exception], ...]:
                return (requests_gssapi.exceptions.SPNEGOExchangeError,)

        try:
            connection = connect(
                host=host,
                port=self.config.trino_port,
                user=self.config.trino_user,
                catalog=self.config.catalog,
                schema=self.config.schema,
                http_scheme="https",
                auth=CacheAuthentication(),
                verify=self.config.ca_bundle or True,
                request_timeout=self.config.command_timeout,
                max_attempts=1,
                session_properties={"query_max_run_time": f"{self.config.command_timeout}s"},
            )
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute(sql)
                    return cursor.fetchall()
                finally:
                    cursor.close()
            finally:
                connection.close()
        except Exception:
            raise CatalogError(
                "Trino operation failed; check authentication, catalog and rights"
            ) from None


@dataclass(frozen=True)
class TrinoReceipt:
    result_id: str
    table: str
    location: str
    rows: int
    files: int
    content_sha256: str
    reused: bool = False


class TrinoWriter:
    """SQL-only prototype: write a private table, then publish its name once complete."""

    def __init__(self, config: Config, client: TrinoClient) -> None:
        self.config, self.client = config, client
        self.prefix = f'"{config.catalog}"."{config.schema}".'

    def _name(self, result_id: str) -> str:
        result_identifier(result_id)
        return self.config.table + "_" + hashlib.sha256(result_id.encode()).hexdigest()[:32]

    def lookup(self, result_id: str) -> TrinoReceipt | None:
        name = self._name(result_id)
        try:
            exists = self.client.query(
                f'SELECT table_name FROM "{self.config.catalog}".information_schema.tables '
                f"WHERE table_schema = {literal(self.config.schema)} AND table_name = {literal(name)}"
            )
            if not exists:
                return None
            comments = self.client.query(
                "SELECT comment FROM system.metadata.table_comments "
                f"WHERE catalog_name = {literal(self.config.catalog)} "
                f"AND schema_name = {literal(self.config.schema)} AND table_name = {literal(name)}"
            )
            metadata = json.loads(comments[0][0])
            if metadata["version"] != 1 or metadata["result_id"] != result_id:
                raise ValueError("Unexpected table metadata")
            root = self.config.hdfs_root.rstrip("/") + "/trino/"
            if not metadata["location"].startswith(root):
                raise ValueError("Unexpected table location")
            UUID(metadata["location"].removeprefix(root))
            stats = self.client.query(
                f'SELECT count(*), count(DISTINCT "$path") FROM {self.prefix}"{name}"'
            )[0]
            if stats[0] != metadata["rows"] or stats[0] < 1:
                raise ValueError("Published row count mismatch")
            return TrinoReceipt(
                result_id,
                f"{self.config.catalog}.{self.config.schema}.{name}",
                metadata["location"],
                stats[0],
                stats[1],
                metadata["content_sha256"],
                reused=True,
            )
        except (CatalogError, ValueError, KeyError, IndexError, TypeError):
            raise PublicationUnknown(
                "Cannot confirm Trino table; preserve attempts and retry lookup"
            ) from None

    def publish(
        self,
        result_id: str,
        batches: Any,
        *,
        schema: pa.Schema | None = None,
        batch_size: int = 8192,
    ) -> TrinoReceipt:
        batches = as_batches(batches, schema=schema, batch_size=batch_size)
        name = self._name(result_id)
        attempt = uuid4()
        temporary = self.prefix + f'"_hrw_{attempt.hex}"'
        location = f"{self.config.hdfs_root.rstrip('/')}/trino/{attempt}"
        schema = None
        digest = hashlib.sha256()
        rows = 0
        values: list[str] = []
        value_bytes = 0

        def flush() -> None:
            nonlocal value_bytes
            if values:
                # Never retry an ambiguous INSERT: abandon this entire private attempt.
                self.client.query(f"INSERT INTO {temporary} VALUES " + ",".join(values))
                values.clear()
                value_bytes = 0

        for batch in batches:
            if not isinstance(batch, pa.RecordBatch):
                raise ValueError("Input must contain Arrow RecordBatch objects")
            if schema is None:
                schema = batch.schema
                types = {
                    pa.int64(): "BIGINT",
                    pa.int32(): "INTEGER",
                    pa.float64(): "DOUBLE",
                    pa.float32(): "REAL",
                    pa.string(): "VARCHAR",
                    pa.large_string(): "VARCHAR",
                    pa.binary(): "VARBINARY",
                    pa.large_binary(): "VARBINARY",
                    pa.bool_(): "BOOLEAN",
                    pa.date32(): "DATE",
                }
                if not schema.names or len(set(schema.names)) != len(schema):
                    raise ValueError("Empty or duplicate schema columns")
                columns = []
                for field in schema:
                    identifier(field.name)
                    if field.type not in types:
                        raise ValueError(f"Unsupported Trino prototype type: {field.type}")
                    columns.append(f'"{field.name}" {types[field.type]}')
                digest.update(schema.serialize().to_pybytes())
                self.client.query(
                    f"CREATE TABLE {temporary} ({','.join(columns)}) "
                    f"WITH (format='PARQUET', external_location={literal(location)})"
                )
            if not schema.equals(batch.schema, check_metadata=True):
                raise ValueError("Input schema changed")
            for row in batch.to_pylist():
                value = "(" + ",".join(sql_value(row[column]) for column in schema.names) + ")"
                encoded = value.encode()
                if len(encoded) > 128 * 1024:
                    raise ValueError("A row exceeds the 128 KiB SQL prototype limit")
                if value_bytes + len(encoded) > 128 * 1024:
                    flush()
                values.append(value)
                value_bytes += len(encoded) + 1
                digest.update(encoded + b"\n")
                rows += 1
        if rows == 0:
            raise ValueError("Empty results are not supported")
        flush()
        if self.client.query(f"SELECT count(*) FROM {temporary}")[0][0] != rows:
            raise PublicationUnknown("Private Trino table row count mismatch")
        metadata = json.dumps(
            {
                "version": 1,
                "result_id": result_id,
                "location": location,
                "rows": rows,
                "content_sha256": digest.hexdigest(),
            },
            sort_keys=True,
        )
        self.client.query(f"COMMENT ON TABLE {temporary} IS {literal(metadata)}")
        existing = self.lookup(result_id)
        if existing is not None:
            if existing.content_sha256 != digest.hexdigest():
                raise Conflict("result_id already has different SQL content")
            return existing
        try:
            self.client.query(f'ALTER TABLE {temporary} RENAME TO "{name}"')
        except CatalogError:
            pass
        for index in range(self.config.readback_attempts):
            try:
                receipt = self.lookup(result_id)
                if receipt is not None:
                    if receipt.content_sha256 != digest.hexdigest():
                        raise Conflict("A different SQL result won table publication")
                    return replace(receipt, reused=receipt.location != location)
            except PublicationUnknown:
                pass
            if index + 1 < self.config.readback_attempts:
                time.sleep(self.config.readback_delay)
        raise PublicationUnknown("Table rename outcome is unknown; retry lookup")
