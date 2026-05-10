from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import duckdb
import pytest

from src.registry_server import RegistryQueryEngine, make_registry_server
from src.storage.data_registry import DataRegistry
from src.storage.parquet_store import ParquetStore


def test_registry_http_gateway_lists_datasets_events_and_queries_parquet_when_duckdb_file_is_locked(
    tmp_path,
    daily_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())
    registry = DataRegistry(root=tmp_path)

    duckdb_file = tmp_path / "data" / "duckdb" / "quant.duckdb"
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    locked_conn = duckdb.connect(str(duckdb_file))
    try:
        with _registry_server(registry, tmp_path) as base_url:
            datasets = _get_json(f"{base_url}/v1/datasets")
            assert any(
                item["dataset_id"] == "baostock_cn_stock_daily_bar_qfq"
                for item in datasets["datasets"]
            )

            events = _get_json(f"{base_url}/v1/events?since_event_id=0")
            assert events["events"][-1]["dataset_id"] == "baostock_cn_stock_daily_bar_qfq"

            result = _post_json(
                f"{base_url}/v1/query",
                {
                    "dataset_id": "baostock_cn_stock_daily_bar_qfq",
                    "columns": ["date", "code", "close"],
                    "filters": [{"column": "code", "op": "=", "value": "sh.600000"}],
                    "order_by": [{"column": "date", "direction": "desc"}],
                    "limit": 1,
                },
            )
            assert result["row_count"] == 1
            assert result["rows"][0]["code"] == "sh.600000"
            assert result["rows"][0]["close"] == 8.3
    finally:
        locked_conn.close()


def test_registry_http_gateway_rejects_unknown_query_columns(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

    with _registry_server(DataRegistry(root=tmp_path), tmp_path) as base_url:
        with pytest.raises(HTTPError) as exc_info:
            _post_json(
                f"{base_url}/v1/query",
                {
                    "dataset_id": "baostock_cn_stock_daily_bar_qfq",
                    "columns": ["not_a_column"],
                    "limit": 1,
                },
            )
        assert exc_info.value.code == 400


@contextmanager
def _registry_server(registry: DataRegistry, root):
    server = make_registry_server(
        "127.0.0.1",
        0,
        registry=registry,
        query_engine=RegistryQueryEngine(root=root),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _get_json(url: str):
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    request = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))
