from __future__ import annotations

import pyarrow as pa

from src.storage.schema import (
    AKSHARE_DAILY_BAR_SCHEMA,
    AKSHARE_DELIST_SH_SCHEMA,
    AKSHARE_DELIST_SZ_SCHEMA,
    AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA,
    AKSHARE_SPOT_QUOTE_SINA_SCHEMA,
    AKSHARE_VALUATION_EASTMONEY_SCHEMA,
    AKSHARE_YYSJ_EM_SCHEMA,
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA,
    BAOSTOCK_CN_STOCK_BASIC_SCHEMA,
    BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA,
    DAILY_BAR_SCHEMA,
    PIPELINE_CHECKPOINTS_SCHEMA,
)


def test_daily_bar_schema_core_fields() -> None:
    assert DAILY_BAR_SCHEMA.field("date").type == pa.date32()
    assert DAILY_BAR_SCHEMA.field("code").type == pa.string()
    assert DAILY_BAR_SCHEMA.field("volume").type == pa.int64()
    assert DAILY_BAR_SCHEMA.field("amount").type == pa.float64()
    assert DAILY_BAR_SCHEMA.names == [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "prev_close",
        "volume",
        "amount",
        "adjust_flag",
        "turnover_rate",
        "trade_status",
        "pct_change",
        "pe_ttm",
        "pb_mrq",
        "ps_ttm",
        "pcf_ncf_ttm",
        "is_st",
    ]


def test_baostock_cn_stock_basic_and_calendar_date_types() -> None:
    assert BAOSTOCK_CN_STOCK_BASIC_SCHEMA.field("ipo_date").type == pa.date32()
    assert BAOSTOCK_CN_STOCK_BASIC_SCHEMA.field("delist_date").type == pa.date32()
    assert BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA.field("calendar_date").type == pa.date32()


def test_baostock_cn_stock_adjustment_factor_schema_core_fields() -> None:
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA.field("dividend_operate_date").type == pa.date32()
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA.field("forward_adjust_factor").type == pa.float64()
    assert BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA.names == [
        "code",
        "dividend_operate_date",
        "forward_adjust_factor",
        "backward_adjust_factor",
        "adjustment_factor",
    ]


def test_pipeline_checkpoint_schema_core_fields() -> None:
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("start_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("end_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("updated_at").type == pa.timestamp("ms")


def test_akshare_dataset_schema_core_fields() -> None:
    assert AKSHARE_VALUATION_EASTMONEY_SCHEMA.field("date").type == pa.date32()
    assert AKSHARE_VALUATION_EASTMONEY_SCHEMA.field("total_market_cap").type == pa.float64()
    assert AKSHARE_VALUATION_EASTMONEY_SCHEMA.names == [
        "date",
        "code",
        "close",
        "pct_change",
        "total_market_cap",
        "float_market_cap",
        "total_shares",
        "float_shares",
        "pe_ttm",
        "pe_static",
        "pb",
        "peg",
        "pcf",
        "ps",
    ]


def test_akshare_a_stock_schema_core_fields() -> None:
    assert AKSHARE_DELIST_SH_SCHEMA.field("snapshot_date").type == pa.date32()
    assert AKSHARE_DELIST_SZ_SCHEMA.field("snapshot_date").type == pa.date32()
    assert AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA.field("trade_date").type == pa.date32()
    assert AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA.field("source_symbol").type == pa.string()
    assert AKSHARE_SPOT_QUOTE_SINA_SCHEMA.field("is_fallback").type == pa.bool_()
    assert AKSHARE_YYSJ_EM_SCHEMA.field("period_end_date").type == pa.date32()
    assert AKSHARE_YYSJ_EM_SCHEMA.field("symbol").type == pa.string()
    assert AKSHARE_DAILY_BAR_SCHEMA.field("volume").type == pa.int64()
    assert AKSHARE_DAILY_BAR_SCHEMA.names == [
        "date",
        "code",
        "source_symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "amplitude",
        "pct_change",
        "price_change",
        "turnover_rate",
        "adjustment",
        "source_endpoint",
        "quality_status",
        "fetched_at",
    ]
