# Data Registry and Query Gateway

`qdc serve-registry` exposes the local data center as a read-only HTTP interface for other applications. Consumers should use this gateway instead of opening `data/duckdb/quant.duckdb` directly, because the DuckDB file can be locked by writer processes on Windows.

## Start the Gateway

```powershell
qdc serve-registry --host 127.0.0.1 --port 8765
```

The default bind address is localhost only. The gateway reads Parquet files directly through in-memory DuckDB views and does not require shared access to `data/duckdb/quant.duckdb`.

## Registry Files

The registry is stored under `data/registry/`:

- `catalog.json`: all known Dataset definitions from `DATASET_CATALOG`, including schema, view name, source, code format, partition column, and lifecycle.
- `inventory.parquet`: physical storage inventory, including file count, partition count, row count, date bounds, latest partition, and latest pipeline status.
- `events.jsonl`: append-only write events. `event_id` is monotonic, so consumers can resume from the last seen event.

## Discovery

Inspect registry health:

```http
GET http://127.0.0.1:8765/v1/status
```

The response includes total dataset count, managed dataset count, latest inventory refresh time, latest event id, and registry directory.

List all datasets:

```http
GET http://127.0.0.1:8765/v1/datasets
```

Inspect one dataset:

```http
GET http://127.0.0.1:8765/v1/datasets/akshare_cn_stock_daily_bar_unadjusted
```

The dataset detail combines catalog metadata with current inventory fields such as lifecycle, partition column, row count, date bounds, latest partition, and latest pipeline status.

List partitions:

```http
GET http://127.0.0.1:8765/v1/datasets/akshare_cn_stock_daily_bar_unadjusted/partitions
```

Partition rows include partition value, row count, file path, and file modification time.

## Events

Poll events after the last consumed event:

```http
GET http://127.0.0.1:8765/v1/events?since_event_id=120
```

Subscribe to server-sent events:

```http
GET http://127.0.0.1:8765/v1/events/stream?since_event_id=120
```

Each event includes `dataset_id`, `code`, date bounds, `row_count`, `output_path`, `occurred_at`, and `event_id`.

## Query

Use structured query JSON instead of raw SQL:

```json
{
  "dataset_id": "akshare_cn_stock_daily_bar_unadjusted",
  "columns": ["date", "code", "close", "quality_status"],
  "filters": [
    {"column": "code", "op": "=", "value": "600000"},
    {"column": "date", "op": ">=", "value": "2026-01-01"}
  ],
  "order_by": [{"column": "date", "direction": "desc"}],
  "limit": 100
}
```

Supported filter operators are `=`, `!=`, `>`, `>=`, `<`, `<=`, `like`, and `in`. Dataset ids and column names are validated against the catalog before SQL is generated.

If `limit` is omitted, the gateway returns up to 1000 rows. Any requested limit above 50000 is capped at 50000.

## Legacy Datasets

Datasets with `lifecycle = legacy_unmanaged` remain discoverable and queryable if files exist, but new pipelines should not depend on them being refreshed.
