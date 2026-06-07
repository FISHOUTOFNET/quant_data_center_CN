# Data Registry Local Metadata

`DataRegistry` maintains an optional local metadata read model under `data/registry/`.
It is useful for diagnostics, but it is not part of the core daily update,
DuckDB query, research, or backtesting path.

The registry contains:

- `catalog.json`: dataset definitions from `DATASET_CATALOG`, including schema, view name, source, code format, partition column, and lifecycle.
- `inventory.parquet`: physical storage inventory, including file count, partition count, row count, date bounds, latest partition, and latest pipeline status.
- `events.jsonl`: append-only write events with monotonic `event_id` values.

Pipeline, repair, and derived build paths may refresh this metadata on a
best-effort basis after Parquet writes. Registry refresh failures should be
logged and should not block source data updates, derived dataset builds, or
DuckDB view creation.

Core consumers should use the Parquet datasets and DuckDB views directly.
