# Scan reference

These functions are exported from `fsspec_polars` and return `polars.LazyFrame`.

## `scan_db_relation`

```python
scan_db_relation(protocol, path, *, storage_options=None, filesystem=None,
                 schema=None, dialect=None)
```

| Parameter                                   | Meaning                                                       |
| ------------------------------------------- | ------------------------------------------------------------- |
| `protocol: str`                             | fsspec protocol used to construct a database filesystem.      |
| `path: str`                                 | Database relation path, such as `/main/users`.                |
| `storage_options: Mapping \| None`          | Constructor options; defaults to an empty mapping.            |
| `filesystem`                                | Existing filesystem; takes precedence over `storage_options`. |
| `schema: Mapping[str, pl.DataType] \| None` | Source schema. If absent, inferred by a zero-row read.        |
| `dialect: str \| None`                      | SQL dialect override.                                         |

The read path uses `.arrow`; recognized `.parquet`, `.csv`, and `.jsonl` suffixes
are replaced. The backend must return Arrow IPC stream bytes from `cat_file`.
Optimizer hints replace existing `columns`, `where`, and `limit` query parameters;
other query parameters remain. Original values of those three keys are not source
constraints.

## `scan_db_sql`

```python
scan_db_sql(protocol, sql, *, storage_options=None, filesystem=None,
            schema=None, dialect=None)
```

Common parameters have the same meaning as in `scan_db_relation`. `sql` is the
source SQL; surrounding whitespace and one trailing semicolon are removed. The
filesystem must implement `query(sql)` returning a `pyarrow.Table`.

Without `schema`, construction runs:

```sql
SELECT * FROM (<source SQL>) AS __fsspec_polars_schema LIMIT 0
```

When pushdown helpers are available, source SQL can be wrapped with optimizer
projection, predicates, and limits. Otherwise the source query runs unchanged and
Polars operations are applied locally.

SQLite expression and aggregate columns can have different inferred dtypes in a
zero-row query and a populated result. An explicit `schema` supplies the expected
dtypes when the backend cannot infer them consistently.

## `scan_kafka`

```python
scan_kafka(path, *, storage_options=None, filesystem=None)
```

| Parameter                          | Meaning                                                             |
| ---------------------------------- | ------------------------------------------------------------------- |
| `path: str`                        | Topic, such as `kafka://orders`, or one partition, `orders/0`.      |
| `storage_options: Mapping \| None` | fsspec-kafka options, such as `bootstrap.servers` and value format. |
| `filesystem`                       | Existing filesystem; takes precedence over `storage_options`.       |

Requires fsspec-kafka. Construction reads `topic/_meta/watermarks.json` for the
partitions and their offset ranges, and a zero-message read of the first partition for
the schema. Each partition is then read as `topic/partition.arrow?start=&limit=`.

With the `pushdown` extra, predicates narrow those reads. `partition ==` and
`partition.is_in` skip partitions, comparisons on `offset` set `start` and `limit`, and
a lower bound on `timestamp` reads `topic/partition/@timestamp.arrow` instead. Converting
time-zone-aware timestamps needs time zone data, which the extra installs on Windows as
`tzdata`. Other
predicates, including `|`, read whole partitions. The full predicate is always applied
to the rows read. A row limit stops reading further partitions, but each partition is
read whole, because compaction leaves gaps between offsets.

## Dialects and dependencies

| Protocol                       | Inferred dialect |
| ------------------------------ | ---------------- |
| `db+sqlite`                    | `sqlite`         |
| `db+postgres`, `db+postgresql` | `postgres`       |
| `db+mysql`                     | `mysql`          |
| Other                          | None             |

The `pushdown` extra supplies `polars-io-tools` and `sqlglot`. Relation projection
and row-limit hints do not require the extra when no predicate needs local
fallback. SQL rewriting requires the extra.

Database, codec, and expression errors propagate from the underlying libraries.
