from __future__ import annotations

from tests.performance.test_bottleneck_full_stage import _copy_root_for_diagnostics


def test_root_copy_for_diagnostics_copies_only_selected_code_partitions(tmp_path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    (source / "config").mkdir(parents=True)
    (source / "config" / "settings.yaml").write_text("pipeline:\n  background_workers: 4\n", encoding="utf-8")
    selected = source / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600000"
    unselected = source / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600001"
    shared = source / "data" / "parquet" / "baostock_cn_trading_calendar"
    selected.mkdir(parents=True)
    unselected.mkdir(parents=True)
    shared.mkdir(parents=True)
    (selected / "data.parquet").write_text("selected", encoding="utf-8")
    (unselected / "data.parquet").write_text("unselected", encoding="utf-8")
    (shared / "data.parquet").write_text("shared", encoding="utf-8")
    (source / "data" / "duckdb").mkdir(parents=True)
    (source / "data" / "duckdb" / "quant.duckdb").write_text("duckdb", encoding="utf-8")

    _copy_root_for_diagnostics(source, destination, codes=["sh.600000"])

    assert (destination / "config" / "settings.yaml").exists()
    assert (
        destination / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600000" / "data.parquet"
    ).exists()
    assert not (destination / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600001").exists()
    assert (destination / "data" / "parquet" / "baostock_cn_trading_calendar" / "data.parquet").exists()
    assert (destination / "data" / "duckdb" / "quant.duckdb").exists()
