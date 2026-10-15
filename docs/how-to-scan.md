# How to scan relations and SQL

Use a relation scan for an existing table or view. Use a SQL scan for a query that
already defines the source rows. Install the backend and any driver required by
your `fsspec-db` protocol.

## Scan a relation

```python
import polars as pl

from fsspec_polars import scan_db_relation

users = scan_db_relation(
    "db+sqlite",
    "/main/users",
    storage_options={"database": "app.db"},
)
result = users.filter(pl.col("score") > 2).select("name").limit(10).collect()
```

## Reuse a filesystem and schema

Pass `filesystem=` when an application already owns the database connection. Supply
`schema=` to avoid the zero-row schema query during LazyFrame construction:

```python
import fsspec
import polars as pl

from fsspec_polars import scan_db_sql

fs = fsspec.filesystem("db+sqlite", database="app.db")
users = scan_db_sql(
    "db+sqlite",
    "SELECT name, score FROM users",
    filesystem=fs,
    schema={"name": pl.String, "score": pl.Int64},
)
result = users.limit(10).collect()
```

Match the schema to the actual source columns and dtypes. Keep the filesystem alive
until collection completes.

## Enable SQL translation

Install optional pushdown helpers:

```console
python -m pip install 'fsspec-polars[pushdown]'
```

Pass `dialect=` for protocols without an inferred SQL dialect:

```python
users = scan_db_relation(
    "db+duckdb",
    "/main/users",
    storage_options={"database": "warehouse.duckdb"},
    dialect="duckdb",
)
```

Unsupported predicates run locally. See [Query pushdown and materialization](explanation.md)
for the resulting read costs and the [reference](reference.md) for path-query handling.
