"""Public registry of datasets and physical inventory."""

from __future__ import annotations

import json
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.dataset_catalog import DATASET_CATALOG, DatasetDefinition
from src.utils import paths


DATE_COLUMN_PRIORITY = (
    "date",
    "trade_date",
    "calendar_date",
    "snapshot_date",
    "period_end_date",
    "dividend_operate_date",
    "fetched_at",
)
CATALOG_FILE = "catalog.json"
INVENTORY_FILE = "inventory.parquet"
_LOCKS: dict[Path, RLock] = {}
_LOCKS_GUARD = RLock()


REGISTRY_INVENTORY_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string()),
        pa.field("logical_name", pa.string()),
        pa.field("source", pa.string()),
        pa.field("view_name", pa.string()),
        pa.field("partition_column", pa.string()),
        pa.field("code_format", pa.string()),
        pa.field("lifecycle", pa.string()),
        pa.field("parquet_file_count", pa.int64()),
        pa.field("partition_count", pa.int64()),
        pa.field("row_count", pa.int64()),
        pa.field("latest_partition", pa.string()),
        pa.field("min_date", pa.string()),
        pa.field("max_date", pa.string()),
        pa.field("latest_file_mtime", pa.timestamp("ms")),
        pa.field("latest_success_date", pa.string()),
        pa.field("latest_status", pa.string()),
        pa.field("latest_status_updated_at", pa.timestamp("ms")),
        pa.field("updated_at", pa.timestamp("ms")),
    ]
)


class DataRegistry:
    """Maintain a file-backed read model for application-layer discovery."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.parquet_dir = self.root / "data" / "parquet"
        self.registry_dir = self.root / "data" / "registry"
        self.catalog_path = self.registry_dir / CATALOG_FILE
        self.inventory_path = self.registry_dir / INVENTORY_FILE
        self._lock = _lock_for(self.registry_dir)

    def ensure(self) -> None:
        """Create registry files if they do not exist."""

        with self._lock:
            self.registry_dir.mkdir(parents=True, exist_ok=True)
            if not self.catalog_path.exists():
                self.write_catalog()
            if not self.inventory_path.exists():
                self.refresh_inventory()

    def write_catalog(self) -> list[dict[str, Any]]:
        rows = [self._catalog_row(definition) for definition in DATASET_CATALOG.values()]
        with self._lock:
            self.registry_dir.mkdir(parents=True, exist_ok=True)
            self.catalog_path.write_text(
                json.dumps(rows, ensure_ascii=False, indent=2, default=_json_default),
                encoding="utf-8",
            )
        return rows

    def read_catalog(self) -> list[dict[str, Any]]:
        if not self.catalog_path.exists():
            return self.write_catalog()
        return json.loads(self.catalog_path.read_text(encoding="utf-8"))

    def refresh_inventory(
        self,
        dataset_ids: Iterable[str] | None = None,
        status_rows: Iterable[dict[str, Any]] | pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Refresh physical inventory rows for selected datasets."""

        requested = set(dataset_ids or DATASET_CATALOG.keys())
        unknown = requested.difference(DATASET_CATALOG)
        if unknown:
            raise ValueError(f"Unknown dataset ids: {sorted(unknown)}")

        with self._lock:
            self.registry_dir.mkdir(parents=True, exist_ok=True)
            current = self.read_inventory() if self.inventory_path.exists() else _empty_inventory()
            status_by_dataset = self._status_by_dataset(current, status_rows)
            scanned = [
                self._inventory_row(DATASET_CATALOG[dataset_id], status_by_dataset.get(dataset_id, {}))
                for dataset_id in sorted(requested)
            ]
            remaining = current.loc[~current["dataset_id"].astype(str).isin(requested)] if not current.empty else current
            refreshed = pd.concat([remaining, pd.DataFrame(scanned)], ignore_index=True)
            refreshed = _clean_inventory(refreshed).sort_values("dataset_id").reset_index(drop=True)
            pq.write_table(pa.Table.from_pandas(refreshed, schema=REGISTRY_INVENTORY_SCHEMA, preserve_index=False), self.inventory_path)
            return refreshed

    def read_inventory(self) -> pd.DataFrame:
        if not self.inventory_path.exists():
            return _empty_inventory()
        with suppress(Exception):
            return _clean_inventory(pd.read_parquet(self.inventory_path))
        return _empty_inventory()

    def dataset_discovery(self) -> list[dict[str, Any]]:
        self.ensure()
        catalog = {row["dataset_id"]: row for row in self.read_catalog()}
        inventory = self.read_inventory()
        rows: list[dict[str, Any]] = []
        for _, row in inventory.iterrows():
            dataset_id = str(row["dataset_id"])
            item = {**catalog.get(dataset_id, {}), **_series_to_dict(row)}
            rows.append(item)
        return rows

    def dataset_detail(self, dataset_id: str) -> dict[str, Any]:
        if dataset_id not in DATASET_CATALOG:
            raise ValueError(f"Unknown dataset: {dataset_id}")
        matches = [item for item in self.dataset_discovery() if item["dataset_id"] == dataset_id]
        if matches:
            return matches[0]
        return self._catalog_row(DATASET_CATALOG[dataset_id])

    def dataset_partitions(self, dataset_id: str) -> list[dict[str, Any]]:
        if dataset_id not in DATASET_CATALOG:
            raise ValueError(f"Unknown dataset: {dataset_id}")
        definition = DATASET_CATALOG[dataset_id]
        dataset_dir = self.parquet_dir / dataset_id
        if not dataset_dir.exists():
            return []
        partition_column = definition.partition_column
        if not partition_column:
            path = dataset_dir / "data.parquet"
            return [_partition_file_row("", path)] if path.exists() else []
        prefix = f"{partition_column}="
        rows = []
        for directory in sorted(dataset_dir.iterdir(), key=lambda item: item.name):
            if not directory.is_dir() or not directory.name.startswith(prefix):
                continue
            path = directory / "data.parquet"
            if path.exists():
                rows.append(_partition_file_row(directory.name[len(prefix):], path))
        return rows

    def status(self) -> dict[str, Any]:
        self.ensure()
        inventory = self.read_inventory()
        return {
            "dataset_count": len(DATASET_CATALOG),
            "managed_dataset_count": sum(1 for item in DATASET_CATALOG.values() if item.lifecycle == "managed"),
            "inventory_updated_at": _max_timestamp_string(inventory.get("updated_at")) if not inventory.empty else "",
            "registry_dir": str(self.registry_dir),
        }

    def _catalog_row(self, definition: DatasetDefinition) -> dict[str, Any]:
        return {
            "dataset_id": definition.id,
            "logical_name": definition.logical_name,
            "source": definition.source,
            "endpoint": definition.endpoint or "",
            "view_name": definition.view_name or f"v_{definition.id}",
            "partition_column": definition.partition_column or "",
            "partitioned_by_code": definition.partitioned_by_code,
            "code_format": definition.code_format,
            "lifecycle": definition.lifecycle,
            "schema": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in definition.schema
            ],
        }

    def _inventory_row(self, definition: DatasetDefinition, status: dict[str, Any]) -> dict[str, Any]:
        physical = _scan_dataset_dir(self.parquet_dir / definition.id, definition)
        now = datetime.now()
        return {
            "dataset_id": definition.id,
            "logical_name": definition.logical_name,
            "source": definition.source,
            "view_name": definition.view_name or f"v_{definition.id}",
            "partition_column": definition.partition_column or "",
            "code_format": definition.code_format,
            "lifecycle": definition.lifecycle,
            "parquet_file_count": physical["parquet_file_count"],
            "partition_count": physical["partition_count"],
            "row_count": physical["row_count"],
            "latest_partition": physical["latest_partition"],
            "min_date": physical["min_date"],
            "max_date": physical["max_date"],
            "latest_file_mtime": physical["latest_file_mtime"],
            "latest_success_date": status.get("latest_success_date", ""),
            "latest_status": status.get("latest_status", ""),
            "latest_status_updated_at": status.get("latest_status_updated_at"),
            "updated_at": now,
        }

    def _status_by_dataset(
        self,
        current: pd.DataFrame,
        status_rows: Iterable[dict[str, Any]] | pd.DataFrame | None,
    ) -> dict[str, dict[str, Any]]:
        status: dict[str, dict[str, Any]] = {}
        if not current.empty:
            for _, row in current.iterrows():
                dataset_id = str(row["dataset_id"])
                status[dataset_id] = {
                    "latest_success_date": _none_to_empty(row.get("latest_success_date")),
                    "latest_status": _none_to_empty(row.get("latest_status")),
                    "latest_status_updated_at": row.get("latest_status_updated_at"),
                }
        if status_rows is None:
            return status
        frame = pd.DataFrame(status_rows)
        if frame.empty or "dataset" not in frame.columns:
            return status
        work = frame.copy()
        work["_updated_at"] = pd.to_datetime(work.get("updated_at"), errors="coerce")
        work["_last_success_date"] = pd.to_datetime(work.get("last_success_date"), errors="coerce")
        for dataset_id, group in work.groupby("dataset", dropna=False):
            dataset_key = str(dataset_id)
            latest = group.sort_values("_updated_at", na_position="first").iloc[-1]
            success_dates = group["_last_success_date"].dropna()
            status[dataset_key] = {
                "latest_success_date": success_dates.max().date().isoformat() if not success_dates.empty else "",
                "latest_status": _none_to_empty(latest.get("status")),
                "latest_status_updated_at": latest.get("_updated_at"),
            }
        return status


def _scan_dataset_dir(dataset_dir: Path, definition: DatasetDefinition) -> dict[str, Any]:
    parquet_files = [
        path for path in dataset_dir.rglob("*.parquet")
        if path.is_file() and ".tmp.parquet" not in path.name
    ] if dataset_dir.exists() else []
    row_count = 0
    min_date = None
    max_date = None
    latest_mtime = None
    for path in parquet_files:
        metadata = pq.read_metadata(path)
        row_count += int(metadata.num_rows)
        file_min, file_max = _parquet_date_bounds(metadata, definition.schema)
        min_date = _min_string(min_date, file_min)
        max_date = _max_string(max_date, file_max)
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        latest_mtime = max(latest_mtime, mtime) if latest_mtime is not None else mtime
    partitions = _partition_values(dataset_dir, definition.partition_column)
    return {
        "parquet_file_count": len(parquet_files),
        "partition_count": len(partitions),
        "row_count": row_count,
        "latest_partition": max(partitions) if partitions else "",
        "min_date": min_date or "",
        "max_date": max_date or "",
        "latest_file_mtime": latest_mtime,
    }


def _parquet_date_bounds(metadata: pq.FileMetaData, schema: pa.Schema) -> tuple[str | None, str | None]:
    date_column = _date_column(schema)
    if date_column is None:
        return None, None
    column_index = None
    for index in range(metadata.num_columns):
        if metadata.schema.column(index).name == date_column:
            column_index = index
            break
    if column_index is None:
        return None, None
    min_date = None
    max_date = None
    for row_group_index in range(metadata.num_row_groups):
        stats = metadata.row_group(row_group_index).column(column_index).statistics
        if stats is None or stats.min is None or stats.max is None:
            continue
        min_date = _min_string(min_date, _value_to_date_string(stats.min))
        max_date = _max_string(max_date, _value_to_date_string(stats.max))
    return min_date, max_date


def _date_column(schema: pa.Schema) -> str | None:
    names = set(schema.names)
    for name in DATE_COLUMN_PRIORITY:
        if name in names:
            return name
    for field in schema:
        if pa.types.is_date(field.type) or pa.types.is_timestamp(field.type):
            return field.name
    return None


def _partition_values(dataset_dir: Path, partition_column: str | None) -> set[str]:
    if not partition_column or not dataset_dir.exists():
        return set()
    prefix = f"{partition_column}="
    return {
        path.name[len(prefix):]
        for path in dataset_dir.iterdir()
        if path.is_dir() and path.name.startswith(prefix) and (path / "data.parquet").exists()
    }


def _partition_file_row(partition_value: str, path: Path) -> dict[str, Any]:
    metadata = pq.read_metadata(path)
    return {
        "partition_value": partition_value,
        "row_count": int(metadata.num_rows),
        "path": str(path),
        "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="milliseconds"),
    }


def _clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = pd.DataFrame(index=df.index)
    for field in REGISTRY_INVENTORY_SCHEMA:
        if field.name in df.columns:
            series = df[field.name]
        else:
            series = pd.Series(pd.NA, index=df.index, name=field.name)
        if pa.types.is_integer(field.type):
            cleaned[field.name] = pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")
        elif pa.types.is_timestamp(field.type):
            cleaned[field.name] = pd.to_datetime(series, errors="coerce").dt.floor("ms")
        else:
            cleaned[field.name] = series.fillna("").astype("string")
    return cleaned.reset_index(drop=True)


def _empty_inventory() -> pd.DataFrame:
    return _clean_inventory(pd.DataFrame(columns=[field.name for field in REGISTRY_INVENTORY_SCHEMA]))


def _series_to_dict(row: pd.Series) -> dict[str, Any]:
    return {key: _jsonable(value) for key, value in row.to_dict().items()}


def _jsonable(value: Any) -> Any:
    if pd.isna(value):
        return ""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    return str(value)


def _value_to_date_string(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def _lock_for(path: Path) -> RLock:
    resolved = path.resolve()
    with _LOCKS_GUARD:
        lock = _LOCKS.get(resolved)
        if lock is None:
            lock = RLock()
            _LOCKS[resolved] = lock
        return lock


def _min_string(left: str | None, right: str | None) -> str | None:
    if not right:
        return left
    if not left:
        return right
    return min(left, right)


def _max_string(left: str | None, right: str | None) -> str | None:
    if not right:
        return left
    if not left:
        return right
    return max(left, right)


def _none_to_empty(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    return str(value)


def _max_timestamp_string(series: pd.Series | None) -> str:
    if series is None:
        return ""
    values = pd.to_datetime(series, errors="coerce").dropna()
    if values.empty:
        return ""
    return values.max().isoformat()
