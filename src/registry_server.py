"""Read-only HTTP gateway for dataset discovery and Parquet queries."""

from __future__ import annotations

import json
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pandas as pd

from src.storage.data_registry import DataRegistry
from src.storage.dataset_catalog import DATASET_CATALOG, dataset_definition
from src.storage.duckdb_store import DuckDBStore


ALLOWED_FILTER_OPS = {"=", "!=", ">", ">=", "<", "<=", "like", "in"}
DEFAULT_QUERY_LIMIT = 1000
MAX_QUERY_LIMIT = 50_000


class RegistryQueryEngine:
    """Execute safe structured queries against Parquet-backed in-memory views."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root

    def query(self, request: dict[str, Any]) -> dict[str, Any]:
        dataset_id = str(request.get("dataset_id", "")).strip()
        definition = dataset_definition(dataset_id)
        valid_columns = _valid_columns(dataset_id)
        columns = _requested_columns(request.get("columns"), valid_columns)
        sql, params = self._query_sql(definition.view_name or f"v_{dataset_id}", columns, request, valid_columns)

        with duckdb.connect(":memory:") as conn:
            for view_sql in DuckDBStore(root=self.root).view_sqls():
                conn.execute(view_sql)
            result = conn.execute(sql, params).df()

        return {
            "dataset_id": dataset_id,
            "columns": list(result.columns),
            "row_count": len(result),
            "rows": json.loads(result.to_json(orient="records", date_format="iso")),
        }

    def _query_sql(
        self,
        view_name: str,
        columns: list[str],
        request: dict[str, Any],
        valid_columns: set[str],
    ) -> tuple[str, list[Any]]:
        params: list[Any] = []
        select_sql = ", ".join(_quote_identifier(column) for column in columns)
        sql = f"SELECT {select_sql} FROM {_quote_identifier(view_name)}"
        where_sqls = []
        for item in request.get("filters") or []:
            if not isinstance(item, dict):
                raise ValueError("Each filter must be an object")
            column = _require_column(str(item.get("column", "")), valid_columns)
            op = str(item.get("op", "")).strip().lower()
            if op not in ALLOWED_FILTER_OPS:
                raise ValueError(f"Unsupported filter operator: {op}")
            if op == "in":
                values = item.get("value")
                if not isinstance(values, list) or not values:
                    raise ValueError("IN filters require a non-empty list value")
                placeholders = ", ".join("?" for _ in values)
                where_sqls.append(f"{_quote_identifier(column)} IN ({placeholders})")
                params.extend(values)
            else:
                where_sqls.append(f"{_quote_identifier(column)} {op.upper()} ?")
                params.append(item.get("value"))
        if where_sqls:
            sql += " WHERE " + " AND ".join(where_sqls)

        order_sqls = []
        for item in request.get("order_by") or []:
            if not isinstance(item, dict):
                raise ValueError("Each order_by entry must be an object")
            column = _require_column(str(item.get("column", "")), valid_columns)
            direction = str(item.get("direction", "asc")).strip().lower()
            if direction not in {"asc", "desc"}:
                raise ValueError(f"Unsupported order direction: {direction}")
            order_sqls.append(f"{_quote_identifier(column)} {direction.upper()}")
        if order_sqls:
            sql += " ORDER BY " + ", ".join(order_sqls)

        limit = request.get("limit", DEFAULT_QUERY_LIMIT)
        try:
            resolved_limit = min(max(int(limit), 0), MAX_QUERY_LIMIT)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid query limit: {limit!r}") from exc
        sql += " LIMIT ?"
        params.append(resolved_limit)
        return sql, params


def serve_registry(host: str = "127.0.0.1", port: int = 8765, root: Path | None = None) -> None:
    registry = DataRegistry(root=root)
    registry.ensure()
    server = make_registry_server(host, port, registry=registry, query_engine=RegistryQueryEngine(root=root))
    server.serve_forever()


def make_registry_server(
    host: str,
    port: int,
    registry: DataRegistry,
    query_engine: RegistryQueryEngine,
) -> ThreadingHTTPServer:
    class Handler(RegistryRequestHandler):
        data_registry = registry
        engine = query_engine

    return ThreadingHTTPServer((host, port), Handler)


class RegistryRequestHandler(BaseHTTPRequestHandler):
    data_registry: DataRegistry
    engine: RegistryQueryEngine
    server_version = "QDCRegistryHTTP/1.0"

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            if path == "/v1/datasets":
                self._send_json({"datasets": self.data_registry.dataset_discovery()})
                return
            if path.startswith("/v1/datasets/"):
                self._handle_dataset_get(path)
                return
            if path == "/v1/status":
                self._send_json(self.data_registry.status())
                return
            if path == "/v1/events":
                since = _query_int(query, "since_event_id", 0)
                self._send_json({"events": self.data_registry.read_events(since_event_id=since)})
                return
            if path == "/v1/events/stream":
                since = _query_int(query, "since_event_id", 0)
                self._stream_events(since)
                return
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception as exc:
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/") != "/v1/query":
                self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
                return
            body = self._read_json_body()
            self._send_json(self.engine.query(body))
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception as exc:
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _handle_dataset_get(self, path: str) -> None:
        suffix = path[len("/v1/datasets/"):]
        if suffix.endswith("/partitions"):
            dataset_id = suffix[: -len("/partitions")].strip("/")
            self._send_json({"dataset_id": dataset_id, "partitions": self.data_registry.dataset_partitions(dataset_id)})
            return
        dataset_id = suffix.strip("/")
        self._send_json(self.data_registry.dataset_detail(dataset_id))

    def _stream_events(self, since_event_id: int) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        latest = since_event_id
        while True:
            events = self.data_registry.read_events(since_event_id=latest)
            for event in events:
                latest = max(latest, int(event.get("event_id", latest)))
                payload = "data: " + json.dumps(event, ensure_ascii=False, default=str) + "\n\n"
                self.wfile.write(payload.encode("utf-8"))
                self.wfile.flush()
            time.sleep(1)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw.decode("utf-8"))
        if not isinstance(body, dict):
            raise ValueError("Request body must be a JSON object")
        return body

    def _send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status=status)


def _valid_columns(dataset_id: str) -> set[str]:
    definition = DATASET_CATALOG[dataset_id]
    columns = set(definition.schema.names)
    if definition.partition_column:
        columns.add(definition.partition_column)
    return columns


def _requested_columns(raw_columns: Any, valid_columns: set[str]) -> list[str]:
    if raw_columns in (None, [], ["*"], "*"):
        return [column for column in valid_columns if column]
    if not isinstance(raw_columns, list):
        raise ValueError("columns must be a list")
    return [_require_column(str(column), valid_columns) for column in raw_columns]


def _require_column(column: str, valid_columns: set[str]) -> str:
    if column not in valid_columns:
        raise ValueError(f"Unknown column: {column}")
    return column


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _query_int(query: dict[str, list[str]], key: str, default: int) -> int:
    values = query.get(key)
    if not values:
        return default
    return int(values[0])
