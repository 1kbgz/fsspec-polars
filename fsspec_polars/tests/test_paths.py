import sqlite3
import sys
import types
from urllib.parse import parse_qsl, urlsplit

import fsspec
import polars as pl
import pytest
from fsspec_data import DataSource, DataViewFileSystem

from fsspec_polars import scan_path


@pytest.fixture
def memory():
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pipe_file("/data/orders.csv", b"id,customer,amount\n1,a,10\n2,b,20\n3,a,5\n")
    return fs


def test_scan_path_reads_local_files(tmp_path):
    (tmp_path / "orders.csv").write_text("id,customer\n1,a\n2,b\n")

    frame = scan_path(tmp_path / "orders.csv").filter(pl.col("customer") == "b").collect()

    assert frame.to_dict(as_series=False) == {"id": [2], "customer": ["b"]}


def test_scan_path_reads_the_node_behind_a_rendered_file(memory):
    frame = scan_path("dataview://orders.parquet::memory:///data").filter(pl.col("customer") == "a").select("amount").head(1).collect()

    assert frame.to_dict(as_series=False) == {"amount": [10]}


def test_scan_path_reads_converted_views_through_the_rendered_file(memory):
    if not hasattr(DataViewFileSystem, "converts"):
        pytest.skip("needs fsspec-data with view schema options")
    frame = scan_path(
        "dataview://orders.parquet::memory:///data",
        storage_options={"requested_schema": {"fields": [{"name": "amount", "type": "int64"}]}, "schema_policy": "projection"},
    ).collect()

    assert frame.to_dict(as_series=False) == {"amount": [10, 20, 5]}


def test_scan_path_rejects_files_that_are_not_tables(memory):
    with pytest.raises(ValueError, match="renders metadata, not a table"):
        scan_path("dataview://orders.metadata.json::memory:///data")


def test_scan_path_resolves_mounted_paths(memory, monkeypatch, tmp_path):
    view = DataViewFileSystem("/data", fs=memory)
    mounted = tmp_path / "mnt" / "orders.parquet"
    fuse = types.ModuleType("fsspec_fuse")

    def resolve(path):
        if path != str(mounted):
            raise ValueError(path)
        return view, "orders.parquet"

    fuse.resolve = resolve
    monkeypatch.setitem(sys.modules, "fsspec_fuse", fuse)

    assert scan_path(mounted).select(pl.col("amount").sum()).collect().item() == 35


def test_scan_path_pushes_queries_on_database_views_to_the_database(tmp_path, monkeypatch):
    fsspec_db = pytest.importorskip("fsspec_db")
    if not issubclass(fsspec_db.SQLiteDatabaseFileSystem, DataSource):
        pytest.skip("needs fsspec-db with data source support")
    pytest.importorskip("polars_io_tools")
    database = tmp_path / "shop.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL);
            INSERT INTO orders VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 5);
            """
        )
    reads = []
    cat_file = fsspec_db.SQLiteDatabaseFileSystem.cat_file

    def recording_cat_file(self, path, *args, **kwargs):
        reads.append(path)
        return cat_file(self, path, *args, **kwargs)

    monkeypatch.setattr(fsspec_db.SQLiteDatabaseFileSystem, "cat_file", recording_cat_file)

    frame = (
        scan_path(f"dataview://main/orders.parquet::db+sqlite://{database}", storage_options={"skip_instance_cache": True})
        .filter(pl.col("amount") > 8)
        .select("customer")
        .collect()
    )

    assert frame.to_dict(as_series=False) == {"customer": ["a", "b"]}
    query = dict(parse_qsl(urlsplit(reads[-1]).query))
    assert urlsplit(reads[-1]).path == "/main/orders.arrow"
    assert set(query["columns"].split(",")) == {"customer", "amount"}
    assert "amount" in query["where"]
