from __future__ import annotations

import pyarrow as pa

from src.storage.schema import CALENDAR_SCHEMA, DAILY_K_SCHEMA, PIPELINE_CHECKPOINTS_SCHEMA, STOCK_BASIC_SCHEMA


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


def test_pipeline_checkpoint_schema_core_fields() -> None:
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("start_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("end_date").type == pa.date32()
    assert PIPELINE_CHECKPOINTS_SCHEMA.field("updated_at").type == pa.timestamp("ms")
