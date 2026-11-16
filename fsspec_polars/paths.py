from __future__ import annotations

import os
import queue
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

import fsspec
import polars as pl
import pyarrow as pa
from fsspec_data import DataSource, DataViewFileSystem, FileDataSource
from polars.io.plugins import register_io_source

from .db import _apply_local_frame_ops, _dialect_from_protocol, _normalize_predicate, scan_db_relation


def scan_path(path: str | os.PathLike[str], *, storage_options: Mapping[str, Any] | None = None) -> pl.LazyFrame:
    """Scan the table behind ``path`` as a Polars LazyFrame.

    ``path`` is a local path, which may be on an fsspec-fuse mount, or an fsspec URL such as
    ``dataview://main/orders.parquet::db+sqlite:///shop.db``. Paths rendered by a data view
    resolve to the node they come from, so a scan of a mounted ``orders.parquet`` reads the
    database relation instead of the file. Database relations take projection, predicate,
    and row-limit pushdown through ``scan_db_relation``; other sources apply them locally.
    """
    fs, source_path = _resolve(path, storage_options)
    if isinstance(fs, DataViewFileSystem):
        entry = fs.resolve(source_path)
        if entry.facet != "table":
            raise ValueError(f"{os.fspath(path)} renders {entry.facet}, not a table")
        if not getattr(fs, "converts", False):
            fs, source_path = fs.source, entry.node.path
    protocol = _database_protocol(fs)
    if protocol is not None:
        return scan_db_relation(protocol, source_path, filesystem=fs, dialect=_dialect_from_protocol(protocol))
    if isinstance(fs, DataViewFileSystem):
        return _scan_file(fs, source_path)
    source = fs if isinstance(fs, DataSource) else FileDataSource(fs)
    return _scan_source(source, source_path)


def _resolve(path: str | os.PathLike[str], storage_options: Mapping[str, Any] | None) -> tuple[Any, str]:
    if isinstance(path, os.PathLike) or not ("://" in path or "::" in path):
        local = os.fspath(path)
        try:
            from fsspec_fuse import resolve
        except ImportError:
            pass
        else:
            try:
                return resolve(local)
            except ValueError:
                pass
        path = local
    return fsspec.core.url_to_fs(path, **dict(storage_options or {}))


def _database_protocol(fs: Any) -> str | None:
    protocol = getattr(fs, "protocol", ())
    protocols = protocol if isinstance(protocol, (tuple, list)) else (protocol,)
    return next((protocol for protocol in protocols if protocol.startswith("db+")), None)


def _scan_source(source: DataSource, path: str) -> pl.LazyFrame:
    schema = pl.from_arrow(source.schema(path).empty_table()).schema

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | Sequence[pl.Expr] | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        return _on_one_thread(lambda: _source_frames(source, path, with_columns, _normalize_predicate(predicate), n_rows, batch_size))

    return register_io_source(source_generator, schema=schema)


def _on_one_thread(frames: Callable[[], Iterator[pl.DataFrame]]) -> Iterator[pl.DataFrame]:
    """Runs ``frames`` on one thread of its own and yields what it produces.

    Polars may advance an I/O source from a different worker thread for each batch, and some
    sources only work on the thread that created them: fsspec-data's native streams abort the
    process otherwise, and sqlite3 connections raise."""
    items: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)
    stopped = threading.Event()

    def put(item: tuple[str, Any]) -> bool:
        while not stopped.is_set():
            try:
                items.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    def run() -> None:
        try:
            for frame in frames():
                if not put(("frame", frame)):
                    return
            put(("done", None))
        except Exception as error:  # noqa: BLE001  re-raised by the consumer
            put(("error", error))

    threading.Thread(target=run, daemon=True).start()
    try:
        while True:
            kind, value = items.get()
            if kind == "error":
                raise value
            if kind == "done":
                return
            yield value
    finally:
        stopped.set()


def _source_frames(
    source: DataSource,
    path: str,
    with_columns: list[str] | None,
    predicate: pl.Expr | None,
    n_rows: int | None,
    batch_size: int | None,
) -> Iterator[pl.DataFrame]:
    """Frames of ``path`` with projection, predicate, and row limit applied batch by batch.
    The source applies the row limit itself only when there is no predicate."""
    remaining = n_rows
    stream = source.scan(path, batch_size=batch_size or 1024, row_limit=n_rows if predicate is None else None)
    for batch in stream:
        frame = _apply_local_frame_ops(pl.from_arrow(pa.Table.from_batches([batch])), with_columns, predicate, remaining)
        if remaining is not None:
            remaining -= frame.height
        yield frame
        if remaining is not None and remaining <= 0:
            return


_READERS = {
    "arrow": pl.read_ipc_stream,
    "parquet": pl.read_parquet,
    "csv": pl.read_csv,
    "jsonl": pl.read_ndjson,
    "json": pl.read_json,
}


def _scan_file(fs: Any, path: str) -> pl.LazyFrame:
    """Reads a rendered file in full. Used when the view applies schema options, so its rows
    differ from the source's and cannot be pushed down."""
    form = fs.resolve(path).form
    if form not in _READERS:
        raise ValueError(f"cannot scan {form} files")
    with fs.open(path, "rb") as file:
        return _READERS[form](file).lazy()
