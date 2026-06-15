from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from src.storage.duckdb_store import DuckDBStore


def _write_parquet(root: Path, dataset: str, frame: pd.DataFrame) -> None:
    path = root / "data" / "parquet" / dataset / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _daily_rows(*, include_adjusted: bool = True) -> pd.DataFrame:
    rows = [
        {
            "date": date(2024, 1, 2),
            "security_id": "CN-SH-600000",
            "code": "SH600000",
            "exchange": "SH",
            "name": "PF Bank",
            "adjustment": "unadjusted",
            "open": 10.0,
            "high": 10.8,
            "low": 9.9,
            "close": 10.5,
            "prev_close": 9.8,
            "volume": 1000.0,
            "amount": 10_500.0,
            "turnover_rate": 1.1,
            "pct_change": 7.14,
            "trade_status": "1",
            "is_st": "0",
            "is_active": True,
            "source_dataset": "fixture",
            "source_endpoint": "fixture",
            "quality_status": "ok",
            "updated_at": pd.Timestamp("2024-01-02 18:00:00"),
        },
        {
            "date": date(2024, 1, 3),
            "security_id": "CN-SH-600000",
            "code": "SH600000",
            "exchange": "SH",
            "name": "PF Bank",
            "adjustment": "unadjusted",
            "open": 11.0,
            "high": 11.2,
            "low": 10.6,
            "close": 10.8,
            "prev_close": 10.5,
            "volume": 1100.0,
            "amount": 11_880.0,
            "turnover_rate": 1.2,
            "pct_change": 2.86,
            "trade_status": "1",
            "is_st": "0",
            "is_active": True,
            "source_dataset": "fixture",
            "source_endpoint": "fixture",
            "quality_status": "ok",
            "updated_at": pd.Timestamp("2024-01-03 18:00:00"),
        },
    ]
    if include_adjusted:
        for adjustment, multiplier in (("qfq", 0.9), ("hfq", 1.2)):
            for row in list(rows):
                adjusted = row.copy()
                adjusted["adjustment"] = adjustment
                adjusted["open"] = row["open"] * multiplier
                adjusted["high"] = row["high"] * multiplier
                adjusted["low"] = row["low"] * multiplier
                adjusted["close"] = row["close"] * multiplier
                adjusted["prev_close"] = row["prev_close"] * multiplier
                rows.append(adjusted)
    return pd.DataFrame(rows)


def _write_baostock_daily(root: Path, *, include_adjusted: bool = True) -> None:
    frame = _daily_rows(include_adjusted=include_adjusted)
    dataset_by_adjustment = {
        "unadjusted": "baostock_cn_stock_daily_bar_unadjusted",
        "qfq": "baostock_cn_stock_daily_bar_qfq",
        "hfq": "baostock_cn_stock_daily_bar_hfq",
    }
    for adjustment, dataset in dataset_by_adjustment.items():
        part = frame[frame["adjustment"] == adjustment].copy()
        if part.empty:
            continue
        part["code"] = "sh.600000"
        part["adjust_flag"] = {"unadjusted": "3", "qfq": "1", "hfq": "2"}[adjustment]
        columns = [
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
            "is_st",
        ]
        _write_parquet(root, dataset, part[columns])


def _factor_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividend_operate_date": date(2024, 1, 2),
                "forward_adjust_factor": 0.9,
                "backward_adjust_factor": 1.1,
                "adjustment_factor": 1.0,
            },
            {
                "code": "sh.600000",
                "dividend_operate_date": date(2024, 1, 3),
                "forward_adjust_factor": 0.8,
                "backward_adjust_factor": 1.2,
                "adjustment_factor": 1.2,
            },
        ]
    )


def test_research_daily_bar_view_pivots_adjustments_and_factor(tmp_path: Path) -> None:
    _write_baostock_daily(tmp_path)
    _write_parquet(tmp_path, "baostock_cn_stock_adjustment_factor", _factor_rows())

    DuckDBStore(root=tmp_path).build_views()

    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        duplicate_rows = conn.execute(
            """
            SELECT code, date, count(*) AS n
            FROM v_cn_stock_daily_bar_research
            GROUP BY code, date
            HAVING count(*) > 1
            """
        ).fetchall()
        assert duplicate_rows == []
        row = conn.execute(
            """
            SELECT
                open,
                close,
                qfq_open,
                qfq_close,
                hfq_open,
                hfq_close,
                adjustment_factor,
                forward_adjust_factor,
                backward_adjust_factor,
                corporate_action_flag
            FROM v_cn_stock_daily_bar_research
            WHERE regexp_extract(code, '(\\d{6})', 1) = '600000'
              AND date = DATE '2024-01-03'
            """
        ).fetchone()
    assert row == (11.0, 10.8, 9.9, 9.72, 13.2, 12.96, 1.2, 0.8, 1.2, True)


def test_research_daily_bar_view_allows_missing_adjusted_and_factor_data(tmp_path: Path) -> None:
    _write_baostock_daily(tmp_path, include_adjusted=False)

    DuckDBStore(root=tmp_path).build_views()

    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        row = conn.execute(
            """
            SELECT
                count(*) AS n,
                count(qfq_close) AS qfq_n,
                count(hfq_close) AS hfq_n,
                count(adjustment_factor) AS factor_n
            FROM v_cn_stock_daily_bar_research
            """
        ).fetchone()
    assert row == (2, 0, 0, 0)


def test_build_views_removes_tmp_parquet_before_reading(tmp_path: Path) -> None:
    _write_baostock_daily(tmp_path, include_adjusted=False)
    tmp_file = (
        tmp_path
        / "data"
        / "parquet"
        / "baostock_cn_stock_daily_bar_unadjusted"
        / "stale.tmp.parquet"
    )
    tmp_file.write_text("not a parquet file", encoding="utf-8")

    DuckDBStore(root=tmp_path).build_views()

    assert not tmp_file.exists()
    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        assert conn.execute("SELECT count(*) FROM v_cn_stock_daily_bar_research").fetchone() == (2,)
