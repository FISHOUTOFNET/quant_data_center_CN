"""AkShare crawler client for dataset-specific ODS ingestion."""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from src.storage.schema import STOCK_INSTITUTE_HOLD_SCHEMA, STOCK_VALUE_EM_SCHEMA, field_names
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


PROJECT_CODE_PATTERN = re.compile(r"^(?P<market>sh|sz)\.(?P<symbol>\d{6})$", re.IGNORECASE)


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
class CodeMaps:
    six_to_project: Mapping[str, str]
    project_to_six: Mapping[str, str]


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


INSTITUTE_HOLD_FIELD_ALIASES = {
    "code": ("证券代码", "股票代码", "code"),
    "code_name": ("证券简称", "股票简称", "code_name"),
    "institution_count": ("机构数", "institution_count"),
    "institution_count_change": ("机构数变化", "institution_count_change"),
    "holding_ratio": ("持股比例", "holding_ratio"),
    "holding_ratio_change": ("持股比例增幅", "holding_ratio_change"),
    "float_holding_ratio": ("占流通股比例", "float_holding_ratio"),
    "float_holding_ratio_change": ("占流通股比例增幅", "float_holding_ratio_change"),
}

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


def build_code_maps(stock_basic_df: pd.DataFrame | None) -> CodeMaps:
    """Build reversible 6-digit/project-code maps from local stock_basic data."""

    if stock_basic_df is None or stock_basic_df.empty or "code" not in stock_basic_df.columns:
        return CodeMaps(six_to_project={}, project_to_six={})

    if "type" in stock_basic_df.columns:
        work_df = stock_basic_df[stock_basic_df["type"].astype(str).str.strip() == "1"]
    else:
        work_df = stock_basic_df

    six_to_project: dict[str, str] = {}
    project_to_six: dict[str, str] = {}
    for raw_code in work_df["code"].dropna().astype(str):
        code = raw_code.strip().lower()
        match = PROJECT_CODE_PATTERN.match(code)
        if match is None:
            continue
        symbol = match.group("symbol")
        if symbol in six_to_project and six_to_project[symbol] != code:
            logger.warning(
                "Duplicate 6-digit stock_basic mapping ignored symbol={} existing={} incoming={}",
                symbol,
                six_to_project[symbol],
                code,
            )
            continue
        six_to_project[symbol] = code
        project_to_six[code] = symbol
    return CodeMaps(six_to_project=six_to_project, project_to_six=project_to_six)


def code_to_akshare_symbol(code: str, code_maps: CodeMaps) -> str:
    """Return the AkShare request symbol while leaving stored codes source-shaped."""

    value = str(code).strip()
    if re.fullmatch(r"\d+\.0", value):
        value = value.split(".", 1)[0]
    lower = value.lower()
    mapped = code_maps.project_to_six.get(lower)
    if mapped is not None:
        return mapped
    project_match = PROJECT_CODE_PATTERN.match(lower)
    if project_match is not None:
        return project_match.group("symbol")
    return value.zfill(6) if value.isdigit() else value


def project_code_to_akshare_symbol(code: str, code_maps: CodeMaps) -> str:
    """Backward-compatible wrapper for converting user input to AkShare symbol."""

    return code_to_akshare_symbol(code, code_maps)


def report_period_to_akshare_quarter(report_period: str) -> str:
    value = str(report_period).strip().upper()
    if re.fullmatch(r"\d{4}Q[1-4]", value):
        return f"{value[:4]}{value[-1]}"
    if re.fullmatch(r"\d{4}[1-4]", value):
        return value
    raise ValueError(f"Invalid report period: {report_period}")


def akshare_quarter_to_report_period(quarter: str) -> str:
    value = report_period_to_akshare_quarter(quarter)
    return f"{value[:4]}Q{value[-1]}"


def report_period_end_date(report_period: str) -> date:
    value = akshare_quarter_to_report_period(report_period)
    year = int(value[:4])
    quarter = int(value[-1])
    month_day = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}[quarter]
    return date(year, month_day[0], month_day[1])


def dataframe_hash(df: pd.DataFrame) -> str:
    payload = df.to_json(orient="split", date_format="iso", force_ascii=False, default_handler=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AkShareClient:
    """Dataset-specific AkShare wrapper with mapping, retry, jitter, and circuit breakers."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        stock_basic_df: pd.DataFrame | None = None,
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
        self._code_maps = build_code_maps(stock_basic_df)
        self._states: dict[str, _EndpointState] = {}

    @property
    def code_maps(self) -> CodeMaps:
        return self._code_maps

    def query_stock_institute_hold(self, period: str) -> pd.DataFrame:
        return self.fetch_stock_institute_hold(period).data

    def fetch_stock_institute_hold(self, period: str) -> AkShareResponse:
        report_period = akshare_quarter_to_report_period(period)
        quarter = report_period_to_akshare_quarter(report_period)
        params: dict[str, object] = {"symbol": quarter}
        return self._fetch(
            endpoint="stock_institute_hold",
            params=params,
            caller=lambda: self._ak().stock_institute_hold(symbol=quarter),
            normalizer=lambda raw: self._normalize_stock_institute_hold(raw, report_period),
        )

    def query_stock_value(self, code: str) -> pd.DataFrame:
        return self.fetch_stock_value(code).data

    def fetch_stock_value(self, code: str) -> AkShareResponse:
        symbol = code_to_akshare_symbol(code, self._code_maps)
        params: dict[str, object] = {"symbol": symbol}
        return self._fetch(
            endpoint="stock_value_em",
            params=params,
            caller=lambda: self._ak().stock_value_em(symbol=symbol),
            normalizer=lambda raw: self._normalize_stock_value_em(raw, symbol),
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

    def _normalize_stock_institute_hold(self, raw_df: pd.DataFrame, report_period: str) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        if raw_df.empty:
            raise AkShareEmptyDataError(f"stock_institute_hold returned empty data for {report_period}")
        selected = _select_required_columns(raw_df, INSTITUTE_HOLD_FIELD_ALIASES, "stock_institute_hold")
        selected.insert(0, "period_end_date", report_period_end_date(report_period))
        selected.insert(0, "report_period", report_period)
        selected["code"] = selected["code"].map(_akshare_code_text)
        return selected[field_names(STOCK_INSTITUTE_HOLD_SCHEMA)].reset_index(drop=True)

    def _normalize_stock_value_em(self, raw_df: pd.DataFrame, source_code: str) -> pd.DataFrame:
        raw_df = _standardize_columns(raw_df)
        if raw_df.empty:
            return pd.DataFrame(columns=field_names(STOCK_VALUE_EM_SCHEMA))
        selected = _select_required_columns(raw_df, STOCK_VALUE_FIELD_ALIASES, "stock_value_em")
        selected.insert(1, "code", source_code)
        return selected[field_names(STOCK_VALUE_EM_SCHEMA)].reset_index(drop=True)

    def _endpoint_runtime_config(self, endpoint: str) -> _EndpointRuntimeConfig:
        retries = int(_config_get(self._config, "api.akshare.max_retries", _config_get(self._config, "pipeline.max_retries", 3)))
        raw_jitter = _config_get(self._config, "api.akshare.jitter_seconds", [0, 0])
        jitter = _parse_jitter(raw_jitter)
        threshold = int(_config_get(self._config, f"api.akshare.endpoints.{endpoint}.failure_threshold", 5))
        cooldown_minutes = float(_config_get(self._config, f"api.akshare.endpoints.{endpoint}.cooldown_minutes", 30))
        return _EndpointRuntimeConfig(
            max_retries=max(retries, 1),
            jitter_seconds=jitter,
            failure_threshold=max(threshold, 1),
            cooldown=timedelta(minutes=cooldown_minutes),
        )

    def _raise_if_circuit_open(self, endpoint: str) -> None:
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
        self._states[endpoint] = _EndpointState()

    def _record_failure(self, endpoint: str, runtime: _EndpointRuntimeConfig) -> None:
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
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _akshare_code_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0", text):
        text = text.split(".", 1)[0]
    return text.zfill(6) if text.isdigit() else text


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
