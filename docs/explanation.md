# Query pushdown and materialization

A Polars LazyFrame delays execution, but the database can only reduce its work
when optimizer hints cross the I/O boundary. `fsspec-polars` translates those hints
through the Polars I/O plugin interface.

## Relation paths and SQL take different routes

Relation scans encode selected columns, predicates, and row limits in an
`fsspec-db` path. SQL scans already have a query, so optional helpers rewrite that
query instead. Both return Arrow data that becomes a Polars DataFrame.

## Local fallback preserves filter order

When a predicate cannot be translated, the scan reads the columns needed to filter
locally and delays projection and row limiting until after that filter. Applying
a row limit before an unsupported filter could discard rows that should have
matched.

This preserves results but may require reading the complete relation or query
result. A LazyFrame or a final `.limit()` does not by itself guarantee a bounded
database read.

## Lazy scans still materialize backend results

Relation scans call `cat_file` and decode the returned Arrow IPC bytes. SQL scans
call `query` and receive a complete Arrow table. Frame slicing into requested
batch sizes happens after that materialization; it is not database streaming.

Schema inference can also contact the database during LazyFrame construction.
An explicit schema avoids that query, while making the caller responsible for its
accuracy. Repeated collections can execute the source again.

See [How to scan relations and SQL](how-to-scan.md) for configuration and the
[scan reference](reference.md) for both function contracts.
