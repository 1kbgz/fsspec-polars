# Query a SQLite table with Polars

We will create a SQLite database and collect a filtered Polars LazyFrame.

## Install

Use Python 3.10 or newer:

```console
python -m pip install fsspec-polars
```

## Create a database

Create `scan_demo.py`:

```python
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from fsspec_polars import scan_db_relation

with TemporaryDirectory() as directory:
    database = str(Path(directory, "scores.db"))
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE scores (name TEXT, score INTEGER)")
        connection.executemany(
            "INSERT INTO scores VALUES (?, ?)",
            [("Ada", 10), ("Grace", 12), ("Margaret", 8)],
        )
```

## Scan and filter

Append inside the `with TemporaryDirectory()` block:

```python
    scores = scan_db_relation(
        "db+sqlite",
        "/main/scores",
        storage_options={"database": database},
    )
    result = scores.filter(pl.col("score") > 9).select("name").sort("name").collect()
    print(result.to_dicts())
```

Run `python scan_demo.py`:

```text
[{'name': 'Ada'}, {'name': 'Grace'}]
```

## Collect a SQL scan

Add this import at the top:

```python
from fsspec_polars import scan_db_sql
```

Append inside the temporary-directory block:

```python
    count = scan_db_sql(
        "db+sqlite",
        "SELECT COUNT(*) AS total FROM scores",
        storage_options={"database": database},
        schema={"total": pl.Int64},
    ).collect()
    print(count.to_dicts())
```

The final output is `[{'total': 3}]`. The explicit schema fixes the aggregate result dtype. Both scan functions
returned LazyFrames that Polars executed when collected. See [How to scan relations and SQL](how-to-scan.md)
for application setup.
