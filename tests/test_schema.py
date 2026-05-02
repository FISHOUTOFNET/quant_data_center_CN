from __future__ import annotations

import pyarrow as pa

from src.storage.schema import (
    ADJUST_FACTOR_SCHEMA,
    CALENDAR_SCHEMA,
    DAILY_K_SCHEMA,
    PIPELINE_CHECKPOINTS_SCHEMA,
    STOCK_BASIC_SCHEMA,
    STOCK_INSTITUTE_HOLD_SCHEMA,
    STOCK_VALUE_EM_SCHEMA,
)


def test_daily_k_schema_core_fields() -> None:
    assert DAILY_K_SCHEMA.field("date").type == pa.date32()
    assert DAILY_K_SCHEMA.field("code").type == pa.string()
    assert DAILY_K_SCHEMA.field("volume").type == pa.int64()
    assert DAILY_K_SCHEMA.field("amount").type == pa.float64()
    assert DAILY_K_SCHEMA.names == [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "peTTM",
        "pbMRQ",
        "psTTM",
        "pcfNcfTTM",
        "isST",
    ]


def test_stock_basic_and_calendar_date_types() -> None:
    assert STOCK_BASIC_SCHEMA.field("ipoDate").type == pa.date32()
    assert STOCK_BASIC_SCHEMA.field("outDate").type == pa.date32()
    assert CALENDAR_SCHEMA.field("calendar_date").type == pa.date32()


def test_adjust_factor_schema_core_fields() -> None:
    assert ADJUST_FACTOR_SCHEMA.field("dividOperateDate").type == pa.date32()
    assert ADJUST_FACTOR_SCHEMA.field("foreAdjustFactor").type == pa.float64()
    assert ADJUST_FACTOR_SCHEMA.names == [
        "code",
        "dividOperateDate",
        "foreAdjustFactor",
        "backAdjustFactor",
        "adjustFactor",
    ]


def test_pipeline_checkpoint_schema_core_fields() -> None:
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("start_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("end_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("updated_at").type == pa.timestamp("ms")


def test_akshare_dataset_schema_core_fields() -> None:
    assert STOCK_INSTITUTE_HOLD_SCHEMA.field("period_end_date").type == pa.date32()
    assert STOCK_INSTITUTE_HOLD_SCHEMA.field("institution_count").type == pa.int64()
    assert STOCK_INSTITUTE_HOLD_SCHEMA.names == [
        "report_period",
        "period_end_date",
        "code",
        "code_name",
        "institution_count",
        "institution_count_change",
        "holding_ratio",
        "holding_ratio_change",
        "float_holding_ratio",
        "float_holding_ratio_change",
    ]

    assert STOCK_VALUE_EM_SCHEMA.field("date").type == pa.date32()
    assert STOCK_VALUE_EM_SCHEMA.field("total_market_cap").type == pa.float64()
    assert STOCK_VALUE_EM_SCHEMA.names == [
        "date",
        "code",
        "close",
        "pct_chg",
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
