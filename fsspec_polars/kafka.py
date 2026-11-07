from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import polars as pl
from polars.io.plugins import register_io_source

from .db import _apply_local_frame_ops, _filesystem, _normalize_predicate, _read_arrow_frame, _yield_frame


@dataclass
class _Bounds:
    """Read limits implied by a predicate. Ends are exclusive; `None` means unbounded."""

    partitions: set[int] | None = None
    offset_start: int | None = None
    offset_end: int | None = None
    time_start: datetime | None = None


def scan_kafka(
    path: str,
    *,
    storage_options: Mapping[str, Any] | None = None,
    filesystem: Any | None = None,
) -> pl.LazyFrame:
    """Scan a Kafka topic, or one partition, from fsspec-kafka as a Polars LazyFrame.

    Filters on ``partition``, ``offset``, and ``timestamp`` choose which partitions and
    offsets are read. Row limits stop reading further partitions, but each partition is
    read whole, because compaction leaves gaps between offsets. Every filter is also applied to the rows read, so filters that cannot
    narrow the read still apply. Pushdown needs the ``pushdown`` extra.
    """
    fs = _filesystem("kafka", storage_options, filesystem)
    topic, _, partition = fs._strip_protocol(path).strip("/").partition("/")
    marks = json.loads(fs.cat_file(f"{topic}/_meta/watermarks.json"))
    if partition:
        marks = [mark for mark in marks if mark["partition"] == int(partition)]
    if not marks:
        raise FileNotFoundError(path)
    schema = dict(_read_arrow_frame(fs.cat_file(f"{topic}/{marks[0]['partition']}.arrow?limit=0")).schema)

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | Sequence[pl.Expr] | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        predicate_expr = _normalize_predicate(predicate)
        bounds = _bounds(predicate_expr) if predicate_expr is not None else _Bounds()
        remaining = n_rows
        for mark in marks:
            if remaining == 0:
                return
            read_path = _read_path(topic, mark, bounds)
            if read_path is None:
                continue
            frame = _apply_local_frame_ops(_read_arrow_frame(fs.cat_file(read_path)), with_columns, predicate_expr, remaining)
            if remaining is not None:
                remaining -= frame.height
            yield from _yield_frame(frame, batch_size)

    return register_io_source(source_generator, schema=schema)


def _read_path(topic: str, mark: Mapping[str, int], bounds: _Bounds) -> str | None:
    partition = mark["partition"]
    if bounds.partitions is not None and partition not in bounds.partitions:
        return None
    if bounds.time_start is not None and bounds.offset_start is None:
        # Time reads run to the high watermark; offset ends are applied to the rows read.
        return f"{topic}/{partition}/@{bounds.time_start.isoformat()}.arrow"
    start = max(mark["low"], bounds.offset_start if bounds.offset_start is not None else mark["low"])
    end = min(mark["high"], bounds.offset_end if bounds.offset_end is not None else mark["high"])
    if start >= end:
        return None
    return f"{topic}/{partition}.arrow?start={start}&limit={end - start}"


def _bounds(predicate: pl.Expr) -> _Bounds:
    bounds = _Bounds()
    try:
        from polars_io_tools.io_sources.sql_utils import convert_predicate_to_sql
        from sqlglot import exp
    except ImportError:
        return bounds
    tree = convert_predicate_to_sql(predicate, None)
    if tree is None:
        return bounds
    for condition in _conjuncts(tree, exp):
        _narrow(bounds, condition, exp)
    return bounds


def _conjuncts(tree: Any, exp: Any) -> Iterator[Any]:
    tree = tree.unnest()
    if isinstance(tree, exp.And):
        yield from _conjuncts(tree.this, exp)
        yield from _conjuncts(tree.expression, exp)
    else:
        yield tree


def _narrow(bounds: _Bounds, condition: Any, exp: Any) -> None:
    if isinstance(condition, exp.In) and isinstance(condition.this, exp.Column) and condition.this.name == "partition":
        values = [_literal(value, exp) for value in condition.expressions]
        partitions = {value for value in values if isinstance(value, int)}
        if len(partitions) == len(set(values)):
            bounds.partitions = _intersect(bounds.partitions, partitions)
        return
    flipped = {exp.GT: exp.LT, exp.GTE: exp.LTE, exp.LT: exp.GT, exp.LTE: exp.GTE, exp.EQ: exp.EQ}
    kind = type(condition)
    if kind not in flipped:
        return
    column, value = condition.this, condition.expression
    if isinstance(value, exp.Column):
        column, value, kind = value, column, flipped[kind]
    if not isinstance(column, exp.Column):
        return
    value = _literal(value, exp)
    if column.name == "partition" and kind is exp.EQ and isinstance(value, int):
        bounds.partitions = _intersect(bounds.partitions, {value})
    elif column.name == "offset" and isinstance(value, int):
        start = {exp.EQ: value, exp.GT: value + 1, exp.GTE: value}.get(kind)
        end = {exp.EQ: value + 1, exp.LT: value, exp.LTE: value + 1}.get(kind)
        if start is not None:
            bounds.offset_start = start if bounds.offset_start is None else max(start, bounds.offset_start)
        if end is not None:
            bounds.offset_end = end if bounds.offset_end is None else min(end, bounds.offset_end)
    elif column.name == "timestamp" and kind in (exp.GT, exp.GTE, exp.EQ) and isinstance(value, str):
        try:
            time = datetime.fromisoformat(value)
        except ValueError:
            return
        time = time if time.tzinfo else time.replace(tzinfo=UTC)
        bounds.time_start = time if bounds.time_start is None else max(time, bounds.time_start)


def _literal(value: Any, exp: Any) -> int | str | None:
    if not isinstance(value, exp.Literal):
        return None
    if value.is_string:
        return value.this
    try:
        return int(value.this)
    except ValueError:
        return None


def _intersect(current: set[int] | None, values: set[int]) -> set[int]:
    return values if current is None else current & values
