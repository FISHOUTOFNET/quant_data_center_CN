from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.api.akshare.adapters.financial_report_sina import FinancialReportSinaAdapter
from src.api.akshare.adapters.report_disclosure import report_period_end_date
from src.api.akshare_client import AkShareResponse
from src.pipeline.akshare import AkShareUpdateRequest, update_akshare
from src.pipeline.akshare.modules.financial_report_sina import _resolve_workers, read_financial_report_pending
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


class FakeFinancialReportClient:
    akshare_version = "fake-financial-report"

    def __init__(self, report_dates: dict[str, list[str]] | None = None) -> None:
        self.calls: list[str] = []
        self.report_dates = report_dates or {}

    def fetch_financial_report_sina(self, code: str) -> AkShareResponse:
        self.calls.append(code)
        rows: list[dict[str, object]] = []
        for report_type in ("balance_sheet", "income_statement", "cash_flow_statement"):
            for report_date in self.report_dates.get(code, ["20250331"]):
                rows.append(
                    {
                        "code": code,
                        "source_symbol": f"sh{code}" if code.startswith("6") else f"sz{code}",
                        "report_type": report_type,
                        "report_date": report_date,
                        "period_end_date": report_date,
                        "item_name": "货币资金",
                        "item_value": 100.0,
                        "item_value_text": "100",
                        "data_source": "合并",
                        "is_audit": "否",
                        "publish_date": "2025-04-02",
                        "currency": "人民币",
                        "report_kind": "一季报",
                        "source_update_time": "2025-04-02T20:00:00",
                        "source_endpoint": "stock_financial_report_sina",
                        "fetched_at": datetime(2025, 4, 2, 20, 30),
                    }
                )
        return AkShareResponse(
            endpoint="stock_financial_report_sina",
            params={"code": code},
            akshare_version="fake-financial-report",
            data=pd.DataFrame(rows),
        )


def test_financial_report_sina_adapter_converts_wide_report_to_long_rows() -> None:
    adapter = FinancialReportSinaAdapter("600000", "balance_sheet", fetched_at=datetime(2025, 4, 2, 20, 0))
    raw = pd.DataFrame(
        [
            {
                "报告日": "20250331",
                "货币资金": 100.0,
                "数据源": "合并",
                "是否审计": "否",
                "公告日期": "2025-04-02",
                "币种": "人民币",
                "类型": "一季报",
                "更新日期": "2025-04-02T20:00:00",
            }
        ]
    )

    mapped = adapter.normalize(raw)

    assert adapter.params == {"stock": "sh600000", "symbol": "资产负债表", "code": "600000"}
    assert mapped.loc[0, "code"] == "600000"
    assert mapped.loc[0, "source_symbol"] == "sh600000"
    assert mapped.loc[0, "report_type"] == "balance_sheet"
    assert str(mapped.loc[0, "period_end_date"]) == "2025-03-31"
    assert mapped.loc[0, "item_name"] == "货币资金"
    assert mapped.loc[0, "item_value"] == 100.0
    assert mapped.loc[0, "publish_date"] == pd.Timestamp("2025-04-02").date()


def test_financial_report_sina_normalize_converts_item_columns_once(monkeypatch) -> None:
    calls: list[list[object]] = []

    def fake_to_numeric(series: pd.Series) -> pd.Series:
        calls.append(series.tolist())
        return pd.to_numeric(series, errors="coerce")

    monkeypatch.setattr("src.api.akshare.adapters.financial_report_sina.to_numeric", fake_to_numeric)
    adapter = FinancialReportSinaAdapter("600000", "balance_sheet", fetched_at=datetime(2025, 4, 2, 20, 0))
    raw = pd.DataFrame(
        [
            {"报告日": "20250331", "货币资金": "100", "应收账款": "200", "数据源": "合并"},
            {"报告日": "20241231", "货币资金": "110", "应收账款": "-"},
        ]
    )

    mapped = adapter.normalize(raw)

    assert calls == [["100", "110"], ["200", "-"]]
    assert mapped["item_name"].tolist() == ["货币资金", "应收账款", "货币资金", "应收账款"]
    assert mapped["item_value"].tolist()[:3] == [100, 200, 110]
    assert pd.isna(mapped["item_value"].iloc[3])


def test_financial_report_incremental_uses_local_disclosure_priority_and_pending(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    store.write_dataset(
        "akshare_cn_stock_report_disclosure",
        pd.DataFrame(
            [
                {
                    "report_period": "2025一季",
                    "period_end_date": "2025-03-31",
                    "market": "沪深京",
                    "code": "600000",
                    "name": "浦发银行",
                    "first_scheduled_date": "2025-04-03",
                    "first_changed_date": "2025-04-04",
                    "second_changed_date": None,
                    "third_changed_date": "2025-04-05",
                    "actual_disclosure_date": None,
                    "source_endpoint": "stock_report_disclosure",
                    "fetched_at": datetime(2025, 4, 1, 20, 0),
                }
            ]
        ),
        {"report_period": "2025一季"},
    )
    store.write_dataset(
        "akshare_cn_stock_yysj_em",
        pd.DataFrame(
            [
                {
                    "report_period": "2025一季",
                    "period_end_date": "2025-03-31",
                    "symbol": "沪深A股",
                    "code": "600000",
                    "name": "浦发银行",
                    "first_scheduled_date": "2025-04-03",
                    "first_changed_date": None,
                    "second_changed_date": None,
                    "third_changed_date": None,
                    "actual_disclosure_date": "2025-04-04",
                    "source_endpoint": "stock_yysj_em",
                    "fetched_at": datetime(2025, 4, 1, 20, 0),
                }
            ]
        ),
        {"report_period": "2025一季"},
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20241231"]})

    records = update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="incremental",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2025, 4, 3, 18, 30),
        )
    )

    assert client.calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    pending = read_financial_report_pending(tmp_path)
    assert pending[["code", "report_period", "trigger_date", "trigger_source"]].to_dict("records") == [
        {
            "code": "600000",
            "report_period": "2025一季",
            "trigger_date": "2025-04-04",
            "trigger_source": "stock_yysj_em",
        }
    ]


def test_financial_report_incremental_clears_pending_when_target_period_arrives(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    store.write_dataset(
        "akshare_cn_stock_yysj_em",
        pd.DataFrame(
            [
                {
                    "report_period": "2025一季",
                    "period_end_date": "2025-03-31",
                    "symbol": "沪深A股",
                    "code": "600000",
                    "name": "浦发银行",
                    "first_scheduled_date": "2025-04-03",
                    "first_changed_date": None,
                    "second_changed_date": None,
                    "third_changed_date": None,
                    "actual_disclosure_date": "2025-04-04",
                    "source_endpoint": "stock_yysj_em",
                    "fetched_at": datetime(2025, 4, 1, 20, 0),
                }
            ]
        ),
        {"report_period": "2025一季"},
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20250331"]})

    update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="incremental",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2025, 4, 3, 18, 30),
        )
    )

    loaded = ParquetStore(root=tmp_path).read_dataset("akshare_cn_stock_financial_report_sina", {"code": "600000"})
    assert set(loaded["report_type"].astype(str)) == {"balance_sheet", "income_statement", "cash_flow_statement"}
    assert read_financial_report_pending(tmp_path).empty


def test_financial_report_incremental_ignores_stale_historical_disclosures(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    _write_yysj_disclosure(
        store,
        code="600000",
        report_period="2025\u4e00\u5b63",
        actual_disclosure_date="2025-04-04",
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20250331"]})

    records = update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="incremental",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2026, 6, 3, 18, 30),
        )
    )

    assert records == []
    assert client.calls == []


def test_financial_report_incremental_skips_locally_covered_recent_disclosure(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    _write_yysj_disclosure(
        store,
        code="600000",
        report_period="2026\u4e00\u5b63",
        actual_disclosure_date="2026-06-03",
    )
    store.write_dataset(
        "akshare_cn_stock_financial_report_sina",
        FakeFinancialReportClient(report_dates={"600000": ["20260331"]}).fetch_financial_report_sina("600000").data,
        {"code": "600000"},
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20260331"]})

    records = update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="incremental",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2026, 6, 3, 18, 30),
        )
    )

    assert records == []
    assert client.calls == []


def test_financial_report_full_resume_skips_existing_partition_without_checkpoint(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    store.write_dataset(
        "akshare_cn_stock_financial_report_sina",
        FakeFinancialReportClient(report_dates={"600000": ["20250331"]}).fetch_financial_report_sina("600000").data,
        {"code": "600000"},
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20250331"]})

    records = update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="full",
            root=tmp_path,
            build_views=False,
            client=client,
        )
    )

    assert records == []
    assert client.calls == []


def test_financial_report_full_force_refetches_existing_partition(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_universe(store, spot_codes=("600000",), delisted_codes=())
    store.write_dataset(
        "akshare_cn_stock_financial_report_sina",
        FakeFinancialReportClient(report_dates={"600000": ["20250331"]}).fetch_financial_report_sina("600000").data,
        {"code": "600000"},
    )
    store.close()
    client = FakeFinancialReportClient(report_dates={"600000": ["20250331"]})

    records = update_akshare(
        AkShareUpdateRequest(
            target="financial_report",
            mode="full",
            root=tmp_path,
            build_views=False,
            client=client,
            force=True,
        )
    )

    assert [item["status"] for item in records] == ["success"]
    assert client.calls == ["600000"]


def test_financial_report_update_rejects_existing_lock(tmp_path: Path) -> None:
    _write_settings(tmp_path)
    lock_dir = tmp_path / "data" / "metadata" / "locks"
    lock_dir.mkdir(parents=True)
    (lock_dir / "akshare_financial_report.lock").write_text("pid=1 target=financial_report mode=full\n")

    with pytest.raises(RuntimeError, match="financial_report update is already running"):
        update_akshare(
            AkShareUpdateRequest(
                target="financial_report",
                mode="incremental",
                root=tmp_path,
                build_views=False,
                client=FakeFinancialReportClient(),
            )
        )


def test_financial_report_sina_default_circuit_threshold_handles_transient_resets() -> None:
    config = ConfigManager(Path(__file__).resolve().parents[1])

    threshold = config.get("api.akshare.endpoints.stock_financial_report_sina.failure_threshold")

    assert int(threshold) >= 20


def test_financial_report_sina_uses_dataset_worker_default() -> None:
    config = ConfigManager(Path(__file__).resolve().parents[1])

    assert _resolve_workers(config, None) == 1
    assert _resolve_workers(config, 2) == 2


def _write_universe(store: ParquetStore, spot_codes: tuple[str, ...], delisted_codes: tuple[str, ...]) -> None:
    fetched_at = datetime(2025, 4, 1, 20, 0)
    store.write_dataset(
        "akshare_cn_stock_spot_quote_eastmoney",
        pd.DataFrame(
            [
                {
                    "trade_date": "2025-04-01",
                    "code": code,
                    "source_symbol": code,
                    "name": f"Stock {code}",
                    "last_price": 8.3,
                    "price_change": 0.1,
                    "pct_change": 1.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "prev_close": 8.2,
                    "volume": 120000.0,
                    "amount": 9960.0,
                    "turnover_rate": 0.12,
                    "amplitude": 3.0,
                    "pe_dynamic": 5.1,
                    "pb": 0.71,
                    "total_market_cap": 101000000.0,
                    "float_market_cap": 81000000.0,
                    "source_endpoint": "stock_zh_a_spot_em",
                    "fetched_at": fetched_at,
                }
                for code in spot_codes
            ]
        ),
        {"trade_date": "2025-04-01"},
    )
    if delisted_codes:
        store.write_dataset(
            "akshare_cn_stock_delist_sh",
            pd.DataFrame(
                [
                    {
                        "snapshot_date": "2025-04-01",
                        "exchange": "sh",
                        "market": "全部",
                        "code": code,
                        "source_symbol": code,
                        "name": f"Delisted {code}",
                        "list_date": "2000-01-01",
                        "delist_date": "2025-03-01",
                        "source_endpoint": "stock_info_sh_delist",
                        "fetched_at": fetched_at,
                    }
                    for code in delisted_codes
                ]
            ),
            {"snapshot_date": "2025-04-01"},
        )


def _write_yysj_disclosure(
    store: ParquetStore,
    *,
    code: str,
    report_period: str,
    actual_disclosure_date: str,
) -> None:
    store.write_dataset(
        "akshare_cn_stock_yysj_em",
        pd.DataFrame(
            [
                {
                    "report_period": report_period,
                    "period_end_date": report_period_end_date(report_period),
                    "symbol": "A",
                    "code": code,
                    "name": f"Stock {code}",
                    "first_scheduled_date": None,
                    "first_changed_date": None,
                    "second_changed_date": None,
                    "third_changed_date": None,
                    "actual_disclosure_date": actual_disclosure_date,
                    "source_endpoint": "stock_yysj_em",
                    "fetched_at": datetime(2026, 6, 3, 18, 30),
                }
            ]
        ),
        {"report_period": report_period},
    )


def _write_settings(root: Path) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "project:",
                "  timezone: Asia/Shanghai",
                "api:",
                "  akshare:",
                "    max_retries: 1",
                "    workers: 1",
                "    jitter_seconds: [0, 0]",
                "datasets:",
                "  akshare_cn_stock_financial_report_sina:",
                "    close_after_time: '18:00'",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
