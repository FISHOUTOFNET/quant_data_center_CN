"""Authoritative PyArrow schemas used by Parquet and DuckDB layers."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

DAILY_BAR_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("prev_close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("adjust_flag", pa.string()),
        pa.field("turnover_rate", pa.float64()),
        pa.field("trade_status", pa.string()),
        pa.field("pct_change", pa.float64()),
        pa.field("pe_ttm", pa.float64()),
        pa.field("pb_mrq", pa.float64()),
        pa.field("ps_ttm", pa.float64()),
        pa.field("pcf_ncf_ttm", pa.float64()),
        pa.field("is_st", pa.string()),
    ]
)

BAOSTOCK_VALUATION_PERCENTILE_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("pe_ttm", pa.float64()),
        pa.field("pb_mrq", pa.float64()),
        pa.field("ps_ttm", pa.float64()),
        pa.field("pcf_ncf_ttm", pa.float64()),
        pa.field("pe_ttm_percentile_1y", pa.float64()),
        pa.field("pe_ttm_percentile_3y", pa.float64()),
        pa.field("pe_ttm_percentile_5y", pa.float64()),
        pa.field("pe_ttm_percentile_10y", pa.float64()),
        pa.field("pe_ttm_percentile_all_history", pa.float64()),
        pa.field("pb_mrq_percentile_1y", pa.float64()),
        pa.field("pb_mrq_percentile_3y", pa.float64()),
        pa.field("pb_mrq_percentile_5y", pa.float64()),
        pa.field("pb_mrq_percentile_10y", pa.float64()),
        pa.field("pb_mrq_percentile_all_history", pa.float64()),
        pa.field("ps_ttm_percentile_1y", pa.float64()),
        pa.field("ps_ttm_percentile_3y", pa.float64()),
        pa.field("ps_ttm_percentile_5y", pa.float64()),
        pa.field("ps_ttm_percentile_10y", pa.float64()),
        pa.field("ps_ttm_percentile_all_history", pa.float64()),
        pa.field("pcf_ncf_ttm_percentile_1y", pa.float64()),
        pa.field("pcf_ncf_ttm_percentile_3y", pa.float64()),
        pa.field("pcf_ncf_ttm_percentile_5y", pa.float64()),
        pa.field("pcf_ncf_ttm_percentile_10y", pa.float64()),
        pa.field("pcf_ncf_ttm_percentile_all_history", pa.float64()),
    ]
)

BAOSTOCK_CN_STOCK_BASIC_SCHEMA = pa.schema(
    [
        pa.field("code", pa.string()),
        pa.field("name", pa.string()),
        pa.field("ipo_date", pa.date32()),
        pa.field("delist_date", pa.date32()),
        pa.field("security_type", pa.string()),
        pa.field("listing_status", pa.string()),
    ]
)

BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("calendar_date", pa.date32()),
        pa.field("is_trading_day", pa.string()),
    ]
)

BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA = pa.schema(
    [
        pa.field("code", pa.string()),
        pa.field("dividend_operate_date", pa.date32()),
        pa.field("forward_adjust_factor", pa.float64()),
        pa.field("backward_adjust_factor", pa.float64()),
        pa.field("adjustment_factor", pa.float64()),
    ]
)

AKSHARE_VALUATION_EASTMONEY_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("close", pa.float64()),
        pa.field("pct_change", pa.float64()),
        pa.field("total_market_cap", pa.float64()),
        pa.field("float_market_cap", pa.float64()),
        pa.field("total_shares", pa.float64()),
        pa.field("float_shares", pa.float64()),
        pa.field("pe_ttm", pa.float64()),
        pa.field("pe_static", pa.float64()),
        pa.field("pb", pa.float64()),
        pa.field("peg", pa.float64()),
        pa.field("pcf", pa.float64()),
        pa.field("ps", pa.float64()),
    ]
)

AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA = pa.schema(
    [
        pa.field("change_date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("total_shares", pa.float64()),
        pa.field("restricted_shares", pa.float64()),
        pa.field("other_domestic_restricted_shares", pa.float64()),
        pa.field("domestic_legal_person_restricted_shares", pa.float64()),
        pa.field("domestic_natural_person_restricted_shares", pa.float64()),
        pa.field("circulated_shares", pa.float64()),
        pa.field("listed_a_shares", pa.float64()),
        pa.field("change_reason", pa.string()),
        pa.field("source_endpoint", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

STOCK_INFO_DELIST_SCHEMA = pa.schema(
    [
        pa.field("snapshot_date", pa.date32()),
        pa.field("exchange", pa.string()),
        pa.field("market", pa.string()),
        pa.field("code", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("list_date", pa.date32()),
        pa.field("delist_date", pa.date32()),
        pa.field("source_endpoint", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

AKSHARE_DELIST_SH_SCHEMA = STOCK_INFO_DELIST_SCHEMA
AKSHARE_DELIST_SZ_SCHEMA = STOCK_INFO_DELIST_SCHEMA

AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("last_price", pa.float64()),
        pa.field("price_change", pa.float64()),
        pa.field("pct_change", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("prev_close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("turnover_rate", pa.float64()),
        pa.field("amplitude", pa.float64()),
        pa.field("pe_dynamic", pa.float64()),
        pa.field("pb", pa.float64()),
        pa.field("total_market_cap", pa.float64()),
        pa.field("float_market_cap", pa.float64()),
        pa.field("source_endpoint", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

AKSHARE_SPOT_QUOTE_SINA_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("last_price", pa.float64()),
        pa.field("price_change", pa.float64()),
        pa.field("pct_change", pa.float64()),
        pa.field("bid", pa.float64()),
        pa.field("ask", pa.float64()),
        pa.field("prev_close", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("source_timestamp", pa.string()),
        pa.field("source_endpoint", pa.string()),
        pa.field("is_fallback", pa.bool_()),
        pa.field("fallback_reason", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

AKSHARE_REPORT_DISCLOSURE_SCHEMA = pa.schema(
    [
        pa.field("report_period", pa.string()),
        pa.field("period_end_date", pa.date32()),
        pa.field("market", pa.string()),
        pa.field("code", pa.string()),
        pa.field("name", pa.string()),
        pa.field("first_scheduled_date", pa.date32()),
        pa.field("first_changed_date", pa.date32()),
        pa.field("second_changed_date", pa.date32()),
        pa.field("third_changed_date", pa.date32()),
        pa.field("actual_disclosure_date", pa.date32()),
        pa.field("source_endpoint", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

AKSHARE_DAILY_BAR_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("source_symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("amplitude", pa.float64()),
        pa.field("pct_change", pa.float64()),
        pa.field("price_change", pa.float64()),
        pa.field("turnover_rate", pa.float64()),
        pa.field("adjustment", pa.string()),
        pa.field("source_endpoint", pa.string()),
        pa.field("quality_status", pa.string()),
        pa.field("fetched_at", pa.timestamp("ms")),
    ]
)

AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA = pa.schema(
    [
        pa.field("report_period", pa.string()),
        pa.field("period_end_date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("name", pa.string()),
        pa.field("institution_count", pa.int64()),
        pa.field("institution_count_change", pa.int64()),
        pa.field("holding_ratio", pa.float64()),
        pa.field("holding_ratio_change", pa.float64()),
        pa.field("float_holding_ratio", pa.float64()),
        pa.field("float_holding_ratio_change", pa.float64()),
    ]
)

QLIB_CN_CALENDAR_DAY_SCHEMA = pa.schema(
    [
        pa.field("calendar_date", pa.date32()),
    ]
)

QLIB_CN_INSTRUMENT_MEMBERSHIP_SCHEMA = pa.schema(
    [
        pa.field("universe", pa.string()),
        pa.field("qlib_symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("code", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
    ]
)

QLIB_CN_STOCK_FEATURES_DAY_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("qlib_symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("code", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("factor", pa.float64()),
        pa.field("change", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("adjclose", pa.float64()),
    ]
)

PIPELINE_RUNS_SCHEMA = pa.schema(
    [
        pa.field("task_id", pa.string()),
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("status", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
        pa.field("start_time", pa.timestamp("ms")),
        pa.field("end_time", pa.timestamp("ms")),
        pa.field("row_count", pa.int64()),
        pa.field("error_stack", pa.string()),
    ]
)

DATASET_UPDATE_STATUS_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("last_success_date", pa.date32()),
        pa.field("row_count", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("updated_at", pa.timestamp("ms")),
        pa.field("error_stack", pa.string()),
    ]
)

PIPELINE_CHECKPOINTS_SCHEMA = pa.schema(
    [
        pa.field("pipeline", pa.string()),
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
        pa.field("status", pa.string()),
        pa.field("row_count", pa.int64()),
        pa.field("output_path", pa.string()),
        pa.field("updated_at", pa.timestamp("ms")),
        pa.field("error_stack", pa.string()),
    ]
)

METADATA_SCHEMAS: Mapping[str, pa.Schema] = {
    "pipeline_runs": PIPELINE_RUNS_SCHEMA,
    "dataset_update_status": DATASET_UPDATE_STATUS_SCHEMA,
    "pipeline_checkpoints": PIPELINE_CHECKPOINTS_SCHEMA,
}

DATASET_SCHEMAS: Mapping[str, pa.Schema] = {
    "baostock_cn_stock_daily_bar_unadjusted": DAILY_BAR_SCHEMA,
    "baostock_cn_stock_daily_bar_qfq": DAILY_BAR_SCHEMA,
    "baostock_cn_stock_daily_bar_hfq": DAILY_BAR_SCHEMA,
    "baostock_cn_stock_valuation_percentile": BAOSTOCK_VALUATION_PERCENTILE_SCHEMA,
    "baostock_cn_stock_basic": BAOSTOCK_CN_STOCK_BASIC_SCHEMA,
    "baostock_cn_trading_calendar": BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA,
    "baostock_cn_stock_adjustment_factor": BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA,
    "akshare_cn_stock_valuation_eastmoney": AKSHARE_VALUATION_EASTMONEY_SCHEMA,
    "akshare_cn_stock_capital_structure_em": AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA,
    "akshare_cn_stock_delist_sh": AKSHARE_DELIST_SH_SCHEMA,
    "akshare_cn_stock_delist_sz": AKSHARE_DELIST_SZ_SCHEMA,
    "akshare_cn_stock_spot_quote_eastmoney": AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA,
    "akshare_cn_stock_spot_quote_sina": AKSHARE_SPOT_QUOTE_SINA_SCHEMA,
    "akshare_cn_stock_report_disclosure": AKSHARE_REPORT_DISCLOSURE_SCHEMA,
    "akshare_cn_stock_daily_bar_unadjusted": AKSHARE_DAILY_BAR_SCHEMA,
    "akshare_cn_stock_daily_bar_qfq": AKSHARE_DAILY_BAR_SCHEMA,
    "akshare_cn_stock_daily_bar_hfq": AKSHARE_DAILY_BAR_SCHEMA,
    "akshare_cn_stock_institution_holding": AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA,
    "qlib_cn_calendar_day": QLIB_CN_CALENDAR_DAY_SCHEMA,
    "qlib_cn_instrument_membership": QLIB_CN_INSTRUMENT_MEMBERSHIP_SCHEMA,
    "qlib_cn_stock_features_day": QLIB_CN_STOCK_FEATURES_DAY_SCHEMA,
    **METADATA_SCHEMAS,
}


def schema_for_dataset(dataset: str) -> pa.Schema:
    """Return the schema for a known dataset."""

    try:
        return DATASET_SCHEMAS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset schema: {dataset}") from exc


def field_names(schema: pa.Schema) -> list[str]:
    return [field.name for field in schema]
