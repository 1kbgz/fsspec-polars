from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import fsspec
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
from polars.io.plugins import register_io_source

_PATH_QUERY_KEYS = {"columns", "limit", "where"}
_RELATION_EXTENSIONS = (".arrow", ".parquet", ".csv", ".jsonl")


def scan_db_relation(
    protocol: str,
    path: str,
    *,
    storage_options: Mapping[str, Any] | None = None,
    filesystem: Any | None = None,
    schema: Mapping[str, pl.DataType] | None = None,
    dialect: str | None = None,
) -> pl.LazyFrame:
    """Scan an fsspec-db relation as a Polars LazyFrame."""
    fs = _filesystem(protocol, storage_options, filesystem)
    dialect = dialect or _dialect_from_protocol(protocol)
    source_schema = dict(schema) if schema is not None else _schema_from_relation(fs, path)

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | Sequence[pl.Expr] | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        predicate_expr = _normalize_predicate(predicate)
        predicate_sql = _predicate_to_sql(predicate_expr, dialect) if predicate_expr is not None else None
        predicate_pushed = predicate_expr is None or predicate_sql is not None

        read_path = _relation_arrow_path(
            path,
            columns=with_columns if predicate_pushed else None,
            where=predicate_sql,
            limit=n_rows if predicate_pushed else None,
        )
        frame = _read_arrow_frame(fs.cat_file(read_path))
        frame = _apply_local_frame_ops(frame, with_columns, predicate_expr if not predicate_pushed else None, n_rows)
        yield from _yield_frame(frame, batch_size)

    return register_io_source(source_generator, schema=source_schema)


def scan_db_sql(
    protocol: str,
    sql: str,
    *,
    storage_options: Mapping[str, Any] | None = None,
    filesystem: Any | None = None,
    schema: Mapping[str, pl.DataType] | None = None,
    dialect: str | None = None,
) -> pl.LazyFrame:
    """Scan an fsspec-db SQL query as a Polars LazyFrame."""
    fs = _filesystem(protocol, storage_options, filesystem)
    dialect = dialect or _dialect_from_protocol(protocol)
    source_schema = dict(schema) if schema is not None else _schema_from_sql(fs, sql)

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | Sequence[pl.Expr] | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        predicate_expr = _normalize_predicate(predicate)
        can_push_predicate = predicate_expr is None or _predicate_to_sql(predicate_expr, dialect) is not None

        final_sql = _sql_with_pushdowns(sql, dialect, with_columns, predicate_expr, n_rows, batch_size) if can_push_predicate else None
        pushed = final_sql is not None
        table = fs.query(final_sql or _strip_sql(sql))
        frame = pl.from_arrow(table)
        frame = _apply_local_frame_ops(frame, with_columns, predicate_expr if not pushed else None, n_rows)
        yield from _yield_frame(frame, batch_size)

    return register_io_source(source_generator, schema=source_schema)


def _filesystem(protocol: str, storage_options: Mapping[str, Any] | None, filesystem: Any | None) -> Any:
    if filesystem is not None:
        return filesystem
    return fsspec.filesystem(protocol, **dict(storage_options or {}))


def _dialect_from_protocol(protocol: str) -> str | None:
    return {
        "db+sqlite": "sqlite",
        "db+postgres": "postgres",
        "db+postgresql": "postgres",
        "db+mysql": "mysql",
    }.get(protocol)


def _schema_from_relation(fs: Any, path: str) -> dict[str, pl.DataType]:
    frame = _read_arrow_frame(fs.cat_file(_relation_arrow_path(path, limit=0)))
    return dict(frame.schema)


def _schema_from_sql(fs: Any, sql: str) -> dict[str, pl.DataType]:
    table = fs.query(f"SELECT * FROM ({_strip_sql(sql)}) AS __fsspec_polars_schema LIMIT 0")
    frame = pl.from_arrow(table)
    return dict(frame.schema)


def _relation_arrow_path(
    path: str,
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    parsed = urlsplit(path)
    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key not in _PATH_QUERY_KEYS]
    if columns is not None:
        query.append(("columns", ",".join(columns)))
    if where is not None:
        query.append(("where", where))
    if limit is not None:
        query.append(("limit", str(limit)))
    return urlunsplit(("", "", _with_arrow_extension(parsed.path), urlencode(query, safe=","), parsed.fragment))


def _with_arrow_extension(path: str) -> str:
    for extension in _RELATION_EXTENSIONS:
        if path.endswith(extension):
            return path[: -len(extension)] + ".arrow"
    return f"{path}.arrow"


def _read_arrow_frame(data: bytes | bytearray | memoryview | pa.Buffer) -> pl.DataFrame:
    with ipc.open_stream(data) as reader:
        return pl.from_arrow(reader.read_all())


def _normalize_predicate(predicate: pl.Expr | Sequence[pl.Expr] | None) -> pl.Expr | None:
    if predicate is None:
        return None
    if isinstance(predicate, pl.Expr):
        return predicate
    predicates = list(predicate)
    if not predicates:
        return None
    expr = predicates[0]
    for next_expr in predicates[1:]:
        expr = expr & next_expr
    return expr


def _predicate_to_sql(predicate: pl.Expr | None, dialect: str | None) -> str | None:
    if predicate is None:
        return None
    try:
        from polars_io_tools.io_sources.sql_utils import convert_predicate_to_sql
    except ImportError:
        return None

    sql_expr = convert_predicate_to_sql(predicate, dialect)
    if sql_expr is None:
        return None
    return sql_expr.sql(dialect=dialect)


def _sql_with_pushdowns(
    sql: str,
    dialect: str | None,
    with_columns: list[str] | None,
    predicate: pl.Expr | None,
    n_rows: int | None,
    batch_size: int | None,
) -> str | None:
    try:
        from polars_io_tools.io_sources.sql_utils import apply_polars_io_source_exprs
        from sqlglot import parse_one
        from sqlglot.errors import ParseError
    except ImportError:
        return None

    try:
        parsed = parse_one(_strip_sql(sql), dialect=dialect)
        pushed = apply_polars_io_source_exprs(parsed, dialect, with_columns, predicate, n_rows, batch_size)
        return pushed.sql(dialect=dialect)
    except ParseError:
        return None


def _apply_local_frame_ops(
    frame: pl.DataFrame,
    with_columns: list[str] | None,
    predicate: pl.Expr | None,
    n_rows: int | None,
) -> pl.DataFrame:
    if predicate is not None:
        frame = frame.filter(predicate)
    if with_columns is not None:
        frame = frame.select(with_columns)
    if n_rows is not None:
        frame = frame.head(n_rows)
    return frame


def _yield_frame(frame: pl.DataFrame, batch_size: int | None) -> Iterator[pl.DataFrame]:
    if batch_size is None or batch_size <= 0 or frame.height == 0:
        yield frame
        return
    yield from frame.iter_slices(n_rows=batch_size)


def _strip_sql(sql: str) -> str:
    return sql.strip().removesuffix(";")
