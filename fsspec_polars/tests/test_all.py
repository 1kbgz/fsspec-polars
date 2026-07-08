import sqlite3
from urllib.parse import parse_qsl, urlsplit

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

import fsspec_polars.db as db_mod
from fsspec_polars import scan_db_relation, scan_db_sql


def arrow_stream_bytes(table):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class FakeRelationFileSystem:
    def __init__(self):
        self.paths = []
        self.frame = pl.DataFrame(
            {
                "name": ["ada", "grace", "katherine"],
                "score": [1.0, 2.0, 3.0],
            }
        )

    def cat_file(self, path):
        self.paths.append(path)
        params = dict(parse_qsl(urlsplit(path).query))
        frame = self.frame
        if params.get("where") == "score > 1":
            frame = frame.filter(pl.col("score") > 1)
        if "columns" in params:
            frame = frame.select(params["columns"].split(","))
        if "limit" in params:
            frame = frame.head(int(params["limit"]))
        return arrow_stream_bytes(frame.to_arrow())


class FakeSqlFileSystem:
    def __init__(self):
        self.queries = []

    def query(self, sql, params=None):
        self.queries.append((sql, params))
        if "LIMIT 0" in sql:
            return pa.table({"name": pa.array([], pa.string()), "score": pa.array([], pa.float64())})
        if sql == "SELECT name FROM pushed LIMIT 1":
            return pa.table({"name": ["ada"]})
        return pa.table({"name": ["ada", "grace"], "score": [1.0, 2.0]})


def test_scan_db_relation_pushes_projection_and_limit():
    fs = FakeRelationFileSystem()

    frame = scan_db_relation("db+sqlite", "/main/users", filesystem=fs).select("name").head(1).collect()

    assert frame.to_dict(as_series=False) == {"name": ["ada"]}
    assert fs.paths == [
        "/main/users.arrow?limit=0",
        "/main/users.arrow?columns=name&limit=1",
    ]


def test_scan_db_relation_pushes_supported_predicate(monkeypatch):
    fs = FakeRelationFileSystem()
    monkeypatch.setattr(db_mod, "_predicate_to_sql", lambda predicate, dialect: "score > 1")

    frame = scan_db_relation("db+sqlite", "/main/users", filesystem=fs).filter(pl.col("score") > 1).select("name").collect()

    assert frame.to_dict(as_series=False) == {"name": ["grace", "katherine"]}
    assert fs.paths[-1] == "/main/users.arrow?where=score+%3E+1"


def test_scan_db_relation_filters_locally_when_predicate_is_not_supported(monkeypatch):
    fs = FakeRelationFileSystem()
    monkeypatch.setattr(db_mod, "_predicate_to_sql", lambda predicate, dialect: None)

    frame = scan_db_relation("db+sqlite", "/main/users", filesystem=fs).filter(pl.col("score") > 1).select("name").collect()

    assert frame.to_dict(as_series=False) == {"name": ["grace", "katherine"]}
    assert fs.paths[-1] == "/main/users.arrow"


def test_scan_db_relation_reads_registered_fsspec_db(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (name TEXT NOT NULL, score REAL NOT NULL);
            INSERT INTO users (name, score) VALUES ('ada', 1.0), ('grace', 2.0);
            """
        )

    frame = scan_db_relation("db+sqlite", "/main/users", storage_options={"database": str(path)}).filter(pl.col("score") > 1).select("name").collect()

    assert frame.to_dict(as_series=False) == {"name": ["grace"]}


def test_scan_db_sql_uses_query_pushdown(monkeypatch):
    fs = FakeSqlFileSystem()

    def fake_sql_with_pushdowns(sql, dialect, with_columns, predicate, n_rows, batch_size):
        assert sql == "SELECT name, score FROM users"
        assert dialect == "sqlite"
        assert with_columns == ["name"]
        assert predicate is None
        assert n_rows == 1
        return "SELECT name FROM pushed LIMIT 1"

    monkeypatch.setattr(db_mod, "_sql_with_pushdowns", fake_sql_with_pushdowns)

    frame = scan_db_sql("db+sqlite", "SELECT name, score FROM users", filesystem=fs).select("name").head(1).collect()

    assert frame.to_dict(as_series=False) == {"name": ["ada"]}
    assert fs.queries == [
        ("SELECT * FROM (SELECT name, score FROM users) AS __fsspec_polars_schema LIMIT 0", None),
        ("SELECT name FROM pushed LIMIT 1", None),
    ]


def test_scan_db_sql_filters_locally_when_sql_pushdown_fails(monkeypatch):
    fs = FakeSqlFileSystem()
    monkeypatch.setattr(db_mod, "_predicate_to_sql", lambda predicate, dialect: "score > 1")
    monkeypatch.setattr(db_mod, "_sql_with_pushdowns", lambda sql, dialect, with_columns, predicate, n_rows, batch_size: None)

    frame = scan_db_sql("db+sqlite", "SELECT name, score FROM users", filesystem=fs).filter(pl.col("score") > 1).select("name").collect()

    assert frame.to_dict(as_series=False) == {"name": ["grace"]}
    assert fs.queries == [
        ("SELECT * FROM (SELECT name, score FROM users) AS __fsspec_polars_schema LIMIT 0", None),
        ("SELECT name, score FROM users", None),
    ]


def test_sql_pushdown_helpers_use_polars_io_tools_when_installed():
    pytest.importorskip("polars_io_tools")

    predicate = pl.col("score") > 1
    predicate_sql = db_mod._predicate_to_sql(predicate, "sqlite")
    pushed_sql = db_mod._sql_with_pushdowns("SELECT name, score FROM users", "sqlite", ["name"], predicate, 2, None)

    assert predicate_sql is not None
    assert "score" in predicate_sql
    assert pushed_sql is not None
    normalized = " ".join(pushed_sql.lower().split())
    assert "select name" in normalized
    assert "where" in normalized
    assert "limit 2" in normalized
