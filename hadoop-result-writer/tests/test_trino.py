from __future__ import annotations

import json
import re
from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
import pytest

from hadoop_result_writer.config import Config
from hadoop_result_writer.errors import CatalogError, Conflict, PublicationUnknown
from hadoop_result_writer.trino import TrinoClient, TrinoWriter, literal, sql_value


class FakeSql:
    def __init__(self):
        self.queries = []
        self.published = None
        self.metadata = None
        self.rows = 0
        self.fail_insert = False
        self.lose_rename = False

    def query(self, sql):
        self.queries.append(sql)
        if sql.startswith("INSERT"):
            if self.fail_insert:
                raise CatalogError("ambiguous insert")
            self.rows += sql.split(" VALUES ")[1].count("(")
        elif sql.startswith("COMMENT"):
            self.metadata = json.loads(sql.split(" IS '")[1][:-1].replace("''", "'"))
        elif sql.startswith("ALTER"):
            self.published = self.metadata
            if self.lose_rename:
                raise CatalogError("lost response")
        elif "information_schema.tables" in sql:
            return [["table"]] if self.published else []
        elif "table_comments" in sql:
            return [[json.dumps(self.published)]]
        elif "count(DISTINCT" in sql:
            return [[self.published["rows"], 1]]
        elif "SELECT count(*)" in sql:
            return [[self.rows]]
        return []


@pytest.fixture
def setup():
    config = Config(
        principal="writer@TEST",
        keytab="/keytab",
        hdfs_root="hdfs://nn/root",
        mode="trino",
        readback_attempts=2,
        readback_delay=0,
    )
    sql = FakeSql()
    return sql, TrinoWriter(config, sql)


def source(value=1):
    return [pa.record_batch([[value, value + 1]], names=["value"])]


def test_sql_only_writes_and_publishes_after_inserts(setup):
    sql, writer = setup
    receipt = writer.publish("calc-1", source())
    assert receipt.rows == 2 and not receipt.reused
    operations = [q.split()[0] for q in sql.queries]
    assert operations.index("INSERT") < operations.index("COMMENT") < operations.index("ALTER")
    assert writer.lookup("calc-1").reused
    assert re.fullmatch(r"hive.writer_demo.result_[a-f0-9]{32}", receipt.table)


def test_sql_dataframe_input(setup):
    _, writer = setup
    assert writer.publish("dataframe", pd.DataFrame({"value": ["one", "two"]})).rows == 2


def test_insert_failure_never_publishes_and_is_not_retried(setup):
    sql, writer = setup
    sql.fail_insert = True
    with pytest.raises(CatalogError):
        writer.publish("calc-1", source())
    assert not any(q.startswith("ALTER") for q in sql.queries)
    assert sum(q.startswith("INSERT") for q in sql.queries) == 1


def test_lost_sql_rename_response_is_recovered(setup):
    sql, writer = setup
    sql.lose_rename = True
    assert writer.publish("calc-1", source()).rows == 2


def test_conflicting_sql_result_never_renames(setup):
    sql, writer = setup
    writer.publish("calc-1", source())
    sql.rows = 0
    renames = sum(q.startswith("ALTER") for q in sql.queries)
    with pytest.raises(Conflict):
        writer.publish("calc-1", source(100))
    assert sum(q.startswith("ALTER") for q in sql.queries) == renames


def test_sql_lookup_failure_is_unknown(setup):
    _, writer = setup
    writer.client = Mock()
    writer.client.query.side_effect = CatalogError()
    with pytest.raises(PublicationUnknown):
        writer.lookup("calc-1")


@pytest.mark.parametrize(
    "value,expected",
    [("it's", "'it''s'"), (b"\x00\xff", "X'00ff'"), (None, "NULL"), (True, "TRUE"), (123, "123")],
)
def test_sql_literals(value, expected):
    assert sql_value(value) == expected
    assert literal("' OR 1=1 --") == "''' OR 1=1 --'"


def test_large_sql_row_and_empty_result_rejected(setup):
    sql, writer = setup
    with pytest.raises(ValueError, match="128 KiB"):
        writer.publish("too-big", [pa.record_batch([[b"x" * 131072]], names=["payload"])])
    with pytest.raises(ValueError, match="Empty"):
        writer.publish("empty", [])
    assert not any(q.startswith("ALTER") for q in sql.queries)


def test_sql_batch_boundaries_are_bounded(setup):
    sql, writer = setup
    payloads = [b"x" * 1024] * 100
    receipt = writer.publish("bounded", [pa.record_batch([payloads], names=["payload"])])
    inserts = [query for query in sql.queries if query.startswith("INSERT")]
    assert len(inserts) == 2
    assert receipt.rows == 100
    assert all(len(query.encode()) < 129 * 1024 for query in inserts)


def test_trino_mode_does_not_create_hdfs_adapter_commands(monkeypatch):
    from hadoop_result_writer import Connection

    config = Config(
        principal="writer@TEST", keytab="/keytab", hdfs_root="hdfs://nn/root", mode="trino"
    )
    connection = Connection(config)
    monkeypatch.setattr(connection.kerberos, "ensure", lambda: None)
    query = Mock(return_value=[[1]])
    monkeypatch.setattr(connection.trino, "query", query)
    assert connection.diagnose() == {"kerberos": "ok", "trino": "ok (read-only probe)"}
    query.assert_called_once_with("SELECT 1")


def test_trino_connection_enforces_tls_and_no_query_retries(setup, monkeypatch):
    import trino.dbapi

    _, writer = setup
    auth = Mock(cache="FILE:/private/test-cache")
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [[1]]
    connect = Mock(return_value=connection)
    monkeypatch.setattr(trino.dbapi, "connect", connect)
    assert TrinoClient(writer.config, auth).query("SELECT 1") == [[1]]
    options = connect.call_args.kwargs
    assert options["http_scheme"] == "https"
    assert options["verify"] is True
    assert options["max_attempts"] == 1
    assert options["auth"] is not None
    connection.close.assert_called_once()


def test_sql_fingerprint_is_independent_of_batch_boundaries(setup):
    sql, writer = setup
    original = writer.publish("calc-1", source())
    sql.rows = 0
    repeated = writer.publish(
        "calc-1", [pa.record_batch([[1]], names=["value"]), pa.record_batch([[2]], names=["value"])]
    )
    assert repeated.reused
    assert repeated.content_sha256 == original.content_sha256
