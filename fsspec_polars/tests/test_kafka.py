import json
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlsplit

import polars as pl
import pyarrow as pa
import pytest
from pyarrow import ipc

from fsspec_polars import scan_kafka


def arrow_stream_bytes(frame):
    table = frame.to_arrow()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class FakeKafkaFileSystem:
    """Topic `orders` with two partitions. Compaction removed offsets 1 and 4 from
    partition 0; message timestamps are 1000 ms after their offset in seconds."""

    def __init__(self):
        self.paths = []
        offsets = {0: [0, 2, 3, 5], 1: [0, 1, 2]}
        self.frame = pl.DataFrame(
            {
                "partition": [p for p, values in offsets.items() for _ in values],
                "offset": [o for values in offsets.values() for o in values],
                "timestamp": [datetime.fromtimestamp(1000 + o, UTC) for values in offsets.values() for o in values],
                "value": [f"v{p}-{o}".encode() for p, values in offsets.items() for o in values],
            },
            schema_overrides={"partition": pl.Int32, "timestamp": pl.Datetime("ms", "UTC")},
        )

    def _strip_protocol(self, path):
        return path.removeprefix("kafka://")

    def cat_file(self, path):
        self.paths.append(path)
        if path == "orders/_meta/watermarks.json":
            return json.dumps([{"partition": 0, "low": 0, "high": 6}, {"partition": 1, "low": 0, "high": 3}]).encode()
        parsed = urlsplit(path)
        params = {key: int(value) for key, value in parse_qsl(parsed.query)}
        _, partition, *rest = parsed.path.removesuffix(".arrow").split("/")
        frame = self.frame.filter(pl.col("partition") == int(partition))
        if rest:
            since = datetime.fromisoformat(rest[0].removeprefix("@"))
            frame = frame.filter(pl.col("timestamp") >= since)
        start = params.get("start", 0)
        end = start + params["limit"] if "limit" in params else None
        frame = frame.filter(pl.col("offset") >= start)
        if end is not None:
            frame = frame.filter(pl.col("offset") < end)
        return arrow_stream_bytes(frame)


def data_paths(fs):
    return [path for path in fs.paths if "_meta" not in path and "limit=0" not in path]


def test_scan_kafka_reads_every_partition():
    fs = FakeKafkaFileSystem()

    frame = scan_kafka("kafka://orders", filesystem=fs).collect()

    assert frame.equals(fs.frame)
    assert data_paths(fs) == ["orders/0.arrow?start=0&limit=6", "orders/1.arrow?start=0&limit=3"]


def test_scan_kafka_pushes_partition_and_offset_bounds():
    pytest.importorskip("polars_io_tools")
    fs = FakeKafkaFileSystem()

    frame = scan_kafka("orders", filesystem=fs).filter((pl.col("partition") == 0) & pl.col("offset").is_between(2, 4)).collect()

    assert frame["offset"].to_list() == [2, 3]
    assert data_paths(fs) == ["orders/0.arrow?start=2&limit=3"]

    fs.paths.clear()
    frame = scan_kafka("orders", filesystem=fs).filter(pl.col("partition").is_in([1]) & (pl.col("offset") > 0)).collect()

    assert frame["offset"].to_list() == [1, 2]
    assert data_paths(fs) == ["orders/1.arrow?start=1&limit=2"]


def test_scan_kafka_reads_from_a_timestamp():
    pytest.importorskip("polars_io_tools")
    fs = FakeKafkaFileSystem()
    since = datetime.fromtimestamp(1002, UTC)

    frame = scan_kafka("orders/0", filesystem=fs).filter(pl.col("timestamp") >= since).collect()

    assert frame["offset"].to_list() == [2, 3, 5]
    assert data_paths(fs) == [f"orders/0/@{since.isoformat()}.arrow"]


def test_scan_kafka_filters_what_it_cannot_push_down():
    fs = FakeKafkaFileSystem()

    frame = scan_kafka("orders", filesystem=fs).filter((pl.col("offset") > 2) | (pl.col("partition") == 1)).select("value").collect()

    assert frame["value"].to_list() == [b"v0-3", b"v0-5", b"v1-0", b"v1-1", b"v1-2"]
    assert data_paths(fs) == ["orders/0.arrow?start=0&limit=6", "orders/1.arrow?start=0&limit=3"]


def test_scan_kafka_stops_reading_partitions_at_the_row_limit():
    fs = FakeKafkaFileSystem()

    frame = scan_kafka("orders", filesystem=fs).head(2).collect()

    assert frame["offset"].to_list() == [0, 2]
    assert data_paths(fs) == ["orders/0.arrow?start=0&limit=6"]


def test_scan_kafka_rejects_missing_partitions():
    with pytest.raises(FileNotFoundError):
        scan_kafka("orders/7", filesystem=FakeKafkaFileSystem())


def test_scan_kafka_reads_redpanda():
    import os
    import uuid

    import fsspec

    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP")
    if not bootstrap:
        pytest.skip("KAFKA_TEST_BOOTSTRAP not set")
    pytest.importorskip("fsspec_kafka")
    options = {"bootstrap.servers": bootstrap}
    topic = f"fsspec-polars-{uuid.uuid4().hex}"
    admin = fsspec.filesystem("kafka", **options, allow_admin=True, partitions=2, skip_instance_cache=True)
    admin.mkdir(topic)
    try:
        fs = fsspec.filesystem("kafka", **options, key_column="key", partition_column="partition", skip_instance_cache=True)
        rows = pl.DataFrame({"partition": [0, 0, 1], "key": ["a", "b", "c"], "value": ["1", "2", "3"]})
        with fs.open(f"{topic}.jsonl", "ab") as file:
            rows.write_ndjson(file)

        frame = scan_kafka(f"kafka://{topic}", storage_options=options).filter((pl.col("partition") == 0) & (pl.col("offset") >= 1)).collect()

        assert frame.select("key", "value").to_dict(as_series=False) == {"key": [b"b"], "value": [b"2"]}
    finally:
        admin.rmdir(topic)
