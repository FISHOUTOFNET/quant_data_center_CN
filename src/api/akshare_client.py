"""AkShare crawler client for dataset-specific ODS ingestion."""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from threading import RLock
from typing import Any

import pandas as pd

from src.storage.schema import (
    STOCK_INFO_SH_DELIST_SCHEMA,
    STOCK_INFO_SZ_DELIST_SCHEMA,
    STOCK_VALUE_EM_SCHEMA,
    STOCK_ZH_A_HIST_SCHEMA,
    STOCK_ZH_A_SPOT_EM_SCHEMA,
    STOCK_ZH_A_SPOT_SINA_SCHEMA,
    field_names,
)
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


AKSHARE_PREFIXED_CODE_PATTERN = re.compile(r"^(?P<market>sh|sz|bj)[\.\s_-]?(?P<symbol>\d{6})$", re.IGNORECASE)
AKSHARE_STORAGE_CODE_PATTERN = re.compile(r"^\d{6}$")


class AkShareError(RuntimeError):
    """Base class for AkShare source errors with manifest-friendly typing."""

    error_type = "unknown"


class AkShareNetworkError(AkShareError):
    error_type = "network"


class AkShareCircuitOpen(AkShareError):
    error_type = "circuit_open"


class AkShareSchemaDriftError(AkShareError):
    error_type = "schema_drift"


class AkShareEmptyDataError(AkShareError):
    error_type = "empty_data"


@dataclass(frozen=True)
class AkShareResponse:
    endpoint: str
    params: dict[str, object]
    akshare_version: str
    raw_df: pd.DataFrame
    data: pd.DataFrame
    data_hash: str

    @property
    def row_count(self) -> int:
        return len(self.data)


@dataclass
class _EndpointState:
    consecutive_failures: int = 0
    circuit_open_until: datetime | None = None


@dataclass(frozen=True)
class _EndpointRuntimeConfig:
    max_retries: int
    jitter_seconds: tuple[float, float]
    failure_threshold: int
    cooldown: timedelta


STOCK_VALUE_FIELD_ALIASES = {
    "date": ("数据日期", "date"),
    "close": ("当日收盘价", "close"),
    "pct_chg": ("当日涨跌幅", "pct_chg"),
    "total_market_cap": ("总市值", "total_market_cap"),
    "float_market_cap": ("流通市值", "float_market_cap"),
    "total_shares": ("总股本", "total_shares"),
    "float_shares": ("流通股本", "float_shares"),
    "pe_ttm": ("PE(TTM)", "pe_ttm"),
    "pe_static": ("PE(静)", "pe_static"),
    "pb": ("市净率", "pb"),
    "peg": ("PEG值", "peg"),
    "pcf": ("市现率", "pcf"),
    "ps": ("市销率", "ps"),
}

STOCK_INFO_SH_DELIST_FIELD_ALIASES = {
    "source_symbol": ("公司代码", "证券代码", "代码", "source_symbol"),
    "name": ("公司简称", "证券简称", "名称", "name"),
    "list_date": ("上市日期", "list_date"),
    "delist_date": ("暂停上市日期", "终止上市日期", "delist_date"),
}

STOCK_INFO_SZ_DELIST_FIELD_ALIASES = {
    "source_symbol": ("公司代码", "证券代码", "代码", "source_symbol"),
    "name": ("公司简称", "证券简称", "名称", "name"),
    "list_date": ("上市日期", "list_date"),
    "delist_date": ("终止上市日期", "退市日期", "delist_date"),
}

STOCK_ZH_A_SPOT_EM_FIELD_ALIASES = {
    "source_symbol": ("代码", "股票代码", "source_symbol"),
    "name": ("名称", "股票简称", "name"),
    "latest_price": ("最新价", "最新价格", "latest_price"),
    "change_amount": ("涨跌额", "change_amount"),
    "pct_chg": ("涨跌幅", "pct_chg"),
    "open": ("今开", "开盘", "open"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "preclose": ("昨收", "preclose"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "turnover_rate": ("换手率", "turnover_rate"),
    "amplitude": ("振幅", "amplitude"),
    "pe_dynamic": ("市盈率-动态", "动态市盈率", "pe_dynamic"),
    "pb": ("市净率", "pb"),
    "total_market_cap": ("总市值", "total_market_cap"),
    "float_market_cap": ("流通市值", "float_market_cap"),
}

STOCK_ZH_A_SPOT_SINA_FIELD_ALIASES = {
    "source_symbol": ("代码", "股票代码", "source_symbol"),
    "name": ("名称", "股票简称", "name"),
    "latest_price": ("最新价", "最新价格", "latest_price"),
    "change_amount": ("涨跌额", "change_amount"),
    "pct_chg": ("涨跌幅", "pct_chg"),
    "bid": ("买入", "竞买价", "bid"),
    "ask": ("卖出", "竞卖价", "ask"),
    "preclose": ("昨收", "preclose"),
    "open": ("今开", "开盘", "open"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "source_timestamp": ("时间戳", "时间", "source_timestamp"),
}

STOCK_ZH_A_HIST_FIELD_ALIASES = {
    "date": ("日期", "date"),
    "source_symbol": ("股票代码", "代码", "source_symbol"),
    "open": ("开盘", "open"),
    "close": ("收盘", "close"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "amplitude": ("振幅", "amplitude"),
    "pct_chg": ("涨跌幅", "pct_chg"),
    "change_amount": ("涨跌额", "change_amount"),
    "turnover_rate": ("换手率", "turnover_rate"),
}


def code_to_akshare_symbol(code: object) -> str:
    """Return the 6-digit AkShare request/storage symbol."""

    return normalize_akshare_code(code)


def normalize_akshare_code(code: object) -> str:
    """Validate and return a 6-digit AkShare code for explicit user input."""

    if pd.isna(code):
        raise ValueError("AkShare stock code must be a 6-digit string")
    value = str(code).strip()
    if not AKSHARE_STORAGE_CODE_PATTERN.fullmatch(value):
        raise ValueError(f"AkShare stock code must be 6 digits, got: {value!r}")
    return value


def _normalize_source_code(symbol: object) -> str:
    """Normalize source-provided AkShare/Sina code shapes to storage code."""

    value = _clean_source_symbol(symbol)
    if value == "":
        return ""
    prefixed_match = AKSHARE_PREFIXED_CODE_PATTERN.match(value.lower())
    if prefixed_match is not None:
        return prefixed_match.group("symbol")
    return value.zfill(6) if value.isdigit() else value


def _clean_source_symbol(symbol: object) -> str:
    if pd.isna(symbol):
        return ""
    value = str(symbol).strip()
    if re.fullmatch(r"\d+\.0", value):
        value = value.split(".", 1)[0]
    return value


def dataframe_hash(df: pd.DataFrame) -> str:
    payload = df.to_json(orient="split", date_format="iso", force_ascii=False, default_handler=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AkShareClient:
    """Dataset-specific AkShare wrapper with mapping, retry, jitter, and circuit breakers."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        ak_module: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._ak_module = ak_module
        self._sleep = sleep
        self._random_uniform = random_uniform or _default_random_uniform
        self._now = now or datetime.now
        self._states: dict[str, _EndpointState] = {}
        self._state_lock = RLock()
        self._ak_lock = RLock()

    def query_stock_value(self, code: str) -> pd.DataFrame:
        return self.fetch_stock_value(code).data

    def fetch_stock_value(self, code: str) -> AkShareResponse:
        symbol = code_to_akshare_symbol(code)
        params: dict[str, object] = {"symbol": symbol}
        return self._fetch(
            endpoint="stock_value_em",
            params=params,
            caller=lambda: self._ak().stock_value_em(symbol=symbol),
            normalizer=lambda raw: self._normalize_stock_value_em(raw, symbol),
        )

    def fetch_stock_info_sh_delist(
        self,
        symbol: str = "全部",
        snapshot_date: str | date | None = None,
    ) -> AkShareResponse:
        resolved_snapshot_date = _date_iso(snapshot_date, self._now().date().isoformat())
        params: dict[str, object] = {"symbol": symbol, "snapshot_date": resolved_snapshot_date}
        return self._fetch(
            endpoint="stock_info_sh_delist",
            params=params,
            caller=lambda: self._ak().stock_info_sh_delist(symbol=symbol),
            normalizer=lambda raw: self._normalize_stock_info_sh_delist(
                raw,
                market=symbol,
                snapshot_date=resolved_snapshot_date,
                fetched_at=self._now(),
            ),
        )

    def fetch_stock_info_sz_delist(
        self,
        symbol: str = "终止上市公司",
        snapshot_date: str | date | None = None,
    ) -> AkShareResponse:
        resolved_snapshot_date = _date_iso(snapshot_date, self._now().date().isoformat())
        params: dict[str, object] = {"symbol": symbol, "snapshot_date": resolved_snapshot_date}
        return self._fetch(
            endpoint="stock_info_sz_delist",
            params=params,
            caller=lambda: self._ak().stock_info_sz_delist(symbol=symbol),
            normalizer=lambda raw: self._normalize_stock_info_sz_delist(
                raw,
                market=symbol,
                snapshot_date=resolved_snapshot_date,
                fetched_at=self._now(),
            ),
        )

    def fetch_stock_zh_a_spot_em(self, trade_date: str | date | None = None) -> AkShareResponse:
        resolved_trade_date = _date_iso(trade_date, self._now().date().isoformat())
        params: dict[str, object] = {"trade_date": resolved_trade_date}
        return self._fetch(
            endpoint="stock_zh_a_spot_em",
            params=params,
            caller=lambda: self._ak().stock_zh_a_spot_em(),
            normalizer=lambda raw: self._normalize_stock_zh_a_spot_em(
                raw,
                trade_date=resolved_trade_date,
                fetched_at=self._now(),
            ),
        )

    def fetch_stock_zh_a_spot_sina(
        self,
        trade_date: str | date | None = None,
        fallback_reason: str = "",
    ) -> AkShareResponse:
        resolved_trade_date = _date_iso(trade_date, self._now().date().isoformat())
        params: dict[str, object] = {
            "trade_date": resolved_trade_date,
            "fallback_reason": fallback_reason,
        }
        return self._fetch(
            endpoint="stock_zh_a_spot",
            params=params,
            caller=lambda: self._ak().stock_zh_a_spot(),
            normalizer=lambda raw: self._normalize_stock_zh_a_spot_sina(
                raw,
                trade_date=resolved_trade_date,
                fallback_reason=fallback_reason,
                fetched_at=self._now(),
            ),
        )

    def fetch_stock_zh_a_hist(
        self,
        symbol: str,
        start_date: str | date,
        end_date: str | date,
        adjust: str,
    ) -> AkShareResponse:
        stock_code = code_to_akshare_symbol(symbol)
        normalized_adjust = _normalize_adjust(adjust)
        ak_adjust = "" if normalized_adjust == "none" else normalized_adjust
        request_start = _akshare_date(start_date)
        request_end = _akshare_date(end_date)
        params: dict[str, object] = {
            "symbol": stock_code,
            "code": stock_code,
            "period": "daily",
            "start_date": request_start,
            "end_date": request_end,
            "adjust": normalized_adjust,
        }
        return self._fetch(
            endpoint="stock_zh_a_hist",
            params=params,
            caller=lambda: self._ak().stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=request_start,
                end_date=request_end,
                adjust=ak_adjust,
            ),
            normalizer=lambda raw: self._normalize_stock_zh_a_hist(
                raw,
                stock_code=stock_code,
                source_symbol=stock_code,
                adjust=normalized_adjust,
                fetched_at=self._now(),
            ),
        )

    def _fetch(
        self,
        endpoint: str,
        params: dict[str, object],
        caller: Callable[[], object],
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> AkShareResponse:
        runtime = self._endpoint_runtime_config(endpoint)
        self._raise_if_circuit_open(endpoint)
        attempts = max(runtime.max_retries, 1)
        last_error: AkShareNetworkError | None = None

        for attempt in range(1, attempts + 1):
            self._sleep_jitter(runtime)
            try:
                raw_df = _as_dataframe(caller())
                normalized = normalizer(raw_df)
                self._record_success(endpoint)
                return AkShareResponse(
                    endpoint=endpoint,
                    params=params,
                    akshare_version=self._akshare_version(),
                    raw_df=raw_df,
                    data=normalized,
                    data_hash=dataframe_hash(raw_df),
                )
            except (AkShareSchemaDriftError, AkShareEmptyDataError):
                self._record_failure(endpoint, runtime)
                raise
            except TypeError as exc:
                if "'NoneType' object is not subscriptable" in str(exc):
                    raw_df = pd.DataFrame()
                    normalized = normalizer(raw_df)
                    self._record_success(endpoint)
                    return AkShareResponse(
                        endpoint=endpoint,
                        params=params,
                        akshare_version=self._akshare_version(),
                        raw_df=raw_df,
                        data=normalized,
                        data_hash=dataframe_hash(raw_df),
                    )
                last_error = AkShareNetworkError(f"{endpoint} failed on attempt {attempt}/{attempts}: {exc}")
                if attempt < attempts:
                    logger.warning(
                        "AkShare endpoint={} attempt={}/{} failed: {}",
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                    )
                    continue
                self._record_failure(endpoint, runtime)
            except Exception as exc:
                last_error = AkShareNetworkError(f"{endpoint} failed on attempt {attempt}/{attempts}: {exc}")
                if attempt < attempts:
                    logger.warning(
                        "AkShare endpoint={} attempt={}/{} failed: {}",
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                    )
                    continue
                self._record_failure(endpoint, runtime)

        if last_error is not None:
            raise last_error
        raise AkShareNetworkError(f"{endpoint} failed without a captured error")

    def _normalize_stock_value_em(self, raw_df: pd.DataFrame, source_code: str) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        if raw_df.empty:
            return pd.DataFrame(columns=field_names(STOCK_VALUE_EM_SCHEMA))
        selected = _select_required_columns(raw_df, STOCK_VALUE_FIELD_ALIASES, "stock_value_em")
        selected.insert(1, "code", source_code)
        return selected[field_names(STOCK_VALUE_EM_SCHEMA)].reset_index(drop=True)

    def _normalize_stock_info_sh_delist(
        self,
        raw_df: pd.DataFrame,
        market: str,
        snapshot_date: str,
        fetched_at: datetime,
    ) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        columns = field_names(STOCK_INFO_SH_DELIST_SCHEMA)
        if raw_df.empty:
            return pd.DataFrame(columns=columns)
        selected = _select_required_columns(raw_df, STOCK_INFO_SH_DELIST_FIELD_ALIASES, "stock_info_sh_delist")
        selected["snapshot_date"] = snapshot_date
        selected["exchange"] = "sh"
        selected["market"] = market
        selected["source_symbol"] = selected["source_symbol"].map(_clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(
            _normalize_source_code
        )
        selected["source_endpoint"] = "stock_info_sh_delist"
        selected["fetched_at"] = fetched_at
        return selected[columns].reset_index(drop=True)

    def _normalize_stock_info_sz_delist(
        self,
        raw_df: pd.DataFrame,
        market: str,
        snapshot_date: str,
        fetched_at: datetime,
    ) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        columns = field_names(STOCK_INFO_SZ_DELIST_SCHEMA)
        if raw_df.empty:
            return pd.DataFrame(columns=columns)
        selected = _select_required_columns(raw_df, STOCK_INFO_SZ_DELIST_FIELD_ALIASES, "stock_info_sz_delist")
        selected["snapshot_date"] = snapshot_date
        selected["exchange"] = "sz"
        selected["market"] = market
        selected["source_symbol"] = selected["source_symbol"].map(_clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(
            _normalize_source_code
        )
        selected["source_endpoint"] = "stock_info_sz_delist"
        selected["fetched_at"] = fetched_at
        return selected[columns].reset_index(drop=True)

    def _normalize_stock_zh_a_spot_em(
        self,
        raw_df: pd.DataFrame,
        trade_date: str,
        fetched_at: datetime,
    ) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        if raw_df.empty:
            raise AkShareEmptyDataError("stock_zh_a_spot_em returned empty data")
        selected = _select_required_columns(raw_df, STOCK_ZH_A_SPOT_EM_FIELD_ALIASES, "stock_zh_a_spot_em")
        selected["trade_date"] = trade_date
        selected["source_symbol"] = selected["source_symbol"].map(_clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(
            _normalize_source_code
        )
        for column in [
            "latest_price",
            "change_amount",
            "pct_chg",
            "open",
            "high",
            "low",
            "preclose",
            "volume",
            "amount",
            "turnover_rate",
            "amplitude",
            "pe_dynamic",
            "pb",
            "total_market_cap",
            "float_market_cap",
        ]:
            selected[column] = _to_numeric(selected[column])
        selected["volume"] = selected["volume"] * 100
        selected["source_endpoint"] = "stock_zh_a_spot_em"
        selected["fetched_at"] = fetched_at
        return selected[field_names(STOCK_ZH_A_SPOT_EM_SCHEMA)].reset_index(drop=True)

    def _normalize_stock_zh_a_spot_sina(
        self,
        raw_df: pd.DataFrame,
        trade_date: str,
        fallback_reason: str,
        fetched_at: datetime,
    ) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        if raw_df.empty:
            raise AkShareEmptyDataError("stock_zh_a_spot returned empty data")
        selected = _select_required_columns(raw_df, STOCK_ZH_A_SPOT_SINA_FIELD_ALIASES, "stock_zh_a_spot")
        selected["trade_date"] = trade_date
        selected["source_symbol"] = selected["source_symbol"].map(_clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(
            _normalize_source_code
        )
        for column in [
            "latest_price",
            "change_amount",
            "pct_chg",
            "bid",
            "ask",
            "preclose",
            "open",
            "high",
            "low",
            "volume",
            "amount",
        ]:
            selected[column] = _to_numeric(selected[column])
        selected["source_timestamp"] = selected["source_timestamp"].astype("string")
        selected["source_endpoint"] = "stock_zh_a_spot"
        selected["is_fallback"] = True
        selected["fallback_reason"] = fallback_reason
        selected["fetched_at"] = fetched_at
        return selected[field_names(STOCK_ZH_A_SPOT_SINA_SCHEMA)].reset_index(drop=True)

    def _normalize_stock_zh_a_hist(
        self,
        raw_df: pd.DataFrame,
        stock_code: str,
        source_symbol: str,
        adjust: str,
        fetched_at: datetime,
    ) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        columns = field_names(STOCK_ZH_A_HIST_SCHEMA)
        if raw_df.empty:
            return pd.DataFrame(columns=columns)
        selected = _select_required_columns(raw_df, STOCK_ZH_A_HIST_FIELD_ALIASES, "stock_zh_a_hist")
        selected["source_symbol"] = selected["source_symbol"].map(_clean_source_symbol)
        selected.loc[selected["source_symbol"].astype("string").str.strip() == "", "source_symbol"] = source_symbol
        selected["code"] = selected["source_symbol"].map(
            _normalize_source_code
        )
        selected.loc[selected["code"].astype("string").str.strip() == "", "code"] = stock_code
        for column in [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "amplitude",
            "pct_chg",
            "change_amount",
            "turnover_rate",
        ]:
            selected[column] = _to_numeric(selected[column])
        selected["volume"] = (selected["volume"] * 100).round().astype("Int64")
        selected["adjust"] = adjust
        selected["source_endpoint"] = "stock_zh_a_hist"
        selected["quality_status"] = "hist_confirmed"
        selected["fetched_at"] = fetched_at
        return selected[columns].reset_index(drop=True)

    def _endpoint_runtime_config(self, endpoint: str) -> _EndpointRuntimeConfig:
        retries = int(_config_get(self._config, "api.akshare.max_retries", _config_get(self._config, "pipeline.max_retries", 3)))
        raw_jitter = _config_get(self._config, "api.akshare.jitter_seconds", [0, 0])
        jitter = _parse_jitter(raw_jitter)
        default_threshold = 1 if endpoint == "stock_zh_a_spot" else 5
        default_cooldown_minutes = 180 if endpoint == "stock_zh_a_spot" else 30
        threshold = int(
            _config_get(
                self._config,
                f"api.akshare.endpoints.{endpoint}.failure_threshold",
                default_threshold,
            )
        )
        cooldown_minutes = float(
            _config_get(
                self._config,
                f"api.akshare.endpoints.{endpoint}.cooldown_minutes",
                default_cooldown_minutes,
            )
        )
        return _EndpointRuntimeConfig(
            max_retries=max(retries, 1),
            jitter_seconds=jitter,
            failure_threshold=max(threshold, 1),
            cooldown=timedelta(minutes=cooldown_minutes),
        )

    def _raise_if_circuit_open(self, endpoint: str) -> None:
        with self._state_lock:
            state = self._states.setdefault(endpoint, _EndpointState())
            if state.circuit_open_until is None:
                return
            now = self._now()
            if now < state.circuit_open_until:
                raise AkShareCircuitOpen(
                    f"AkShare endpoint {endpoint} circuit is open until {state.circuit_open_until.isoformat()}"
                )
            state.circuit_open_until = None
            state.consecutive_failures = 0

    def _record_success(self, endpoint: str) -> None:
        with self._state_lock:
            self._states[endpoint] = _EndpointState()

    def _record_failure(self, endpoint: str, runtime: _EndpointRuntimeConfig) -> None:
        with self._state_lock:
            state = self._states.setdefault(endpoint, _EndpointState())
            state.consecutive_failures += 1
            if state.consecutive_failures >= runtime.failure_threshold:
                state.circuit_open_until = self._now() + runtime.cooldown
                logger.warning(
                    "AkShare endpoint={} circuit opened after {} consecutive failures until {}",
                    endpoint,
                    state.consecutive_failures,
                    state.circuit_open_until,
                )

    def _sleep_jitter(self, runtime: _EndpointRuntimeConfig) -> None:
        low, high = runtime.jitter_seconds
        if low <= 0 and high <= 0:
            return
        delay = max(self._random_uniform(low, high), 0.0)
        if delay > 0:
            self._sleep(delay)

    def _ak(self) -> Any:
        if self._ak_module is None:
            with self._ak_lock:
                if self._ak_module is None:
                    try:
                        import akshare as ak  # type: ignore
                    except ModuleNotFoundError as exc:
                        raise AkShareNetworkError("akshare is not installed") from exc
                    self._ak_module = ak
        return self._ak_module

    def _akshare_version(self) -> str:
        return str(getattr(self._ak(), "__version__", "unknown"))


def _default_random_uniform(low: float, high: float) -> float:
    import random

    return random.uniform(low, high)


def _config_get(config: ConfigManager | None, dotted_key: str, default: Any = None) -> Any:
    if config is None:
        return default
    getter = getattr(config, "get", None)
    if getter is None:
        return default
    return getter(dotted_key, default)


def _parse_jitter(raw_jitter: object) -> tuple[float, float]:
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) >= 2:
        low = float(raw_jitter[0])
        high = float(raw_jitter[1])
    else:
        low = high = float(raw_jitter or 0)
    if high < low:
        low, high = high, low
    return low, high


def _as_dataframe(value: object) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _date_iso(value: str | date | datetime | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="raise")
    return parsed.date().isoformat()


def _akshare_date(value: str | date | datetime) -> str:
    return _date_iso(value, datetime.now().date().isoformat()).replace("-", "")


def _normalize_adjust(adjust: str) -> str:
    normalized = str(adjust).strip().lower()
    if normalized in {"", "none", "不复权"}:
        return "none"
    if normalized not in {"qfq", "hfq"}:
        raise ValueError(f"Unsupported stock_zh_a_hist adjust: {adjust}")
    return normalized


def _to_numeric(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_numeric(series, errors="coerce")
    values = series.replace({"": pd.NA, "-": pd.NA, "--": pd.NA, "None": pd.NA, "nan": pd.NA})
    return pd.to_numeric(values, errors="coerce")


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip() for column in result.columns]
    return result


def _select_required_columns(
    df: pd.DataFrame,
    aliases: Mapping[str, tuple[str, ...]],
    endpoint: str,
) -> pd.DataFrame:
    missing: list[str] = []
    selected = pd.DataFrame(index=df.index)
    columns = set(df.columns)
    for target, candidates in aliases.items():
        source = next((candidate for candidate in candidates if candidate in columns), None)
        if source is None:
            missing.append(target)
            continue
        selected[target] = df[source]
    if missing:
        raise AkShareSchemaDriftError(f"{endpoint} missing required fields: {missing}; actual={list(df.columns)}")
    return selected
