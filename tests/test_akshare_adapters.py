from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.api.akshare.adapters.capital_structure_em import CapitalStructureEmAdapter
from src.api.akshare.adapters.daily_bar import DailyBarAdapter
from src.api.akshare.adapters.delist_sh import DelistShAdapter
from src.api.akshare.adapters.report_disclosure import ReportDisclosureAdapter
from src.api.akshare.adapters.spot_quote_eastmoney import SpotQuoteEastmoneyAdapter
from src.api.akshare.adapters.spot_quote_sina import SpotQuoteSinaAdapter
from src.api.akshare.adapters.valuation_eastmoney import ValuationEastmoneyAdapter
from src.api.akshare_client import AkShareEmptyDataError


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload


def test_valuation_eastmoney_adapter_maps_fields_and_empty_schema() -> None:
    adapter = ValuationEastmoneyAdapter("600000")
    raw = pd.DataFrame(
        [
            {
                "数据日期": "2024-01-02",
                "当日收盘价": 8.2,
                "当日涨跌幅": 2.5,
                "总市值": 100.0,
                "流通市值": 80.0,
                "总股本": 12.0,
                "流通股本": 10.0,
                "PE(TTM)": 5.0,
                "PE(静)": 5.5,
                "市净率": 0.7,
                "PEG值": 0.8,
                "市现率": 3.0,
                "市销率": 1.2,
            }
        ]
    )

    mapped = adapter.normalize(raw)
    empty = adapter.normalize(pd.DataFrame())

    assert adapter.endpoint == "stock_value_em"
    assert adapter.params == {"symbol": "600000"}
    assert mapped.loc[0, "code"] == "600000"
    assert mapped.loc[0, "pe_ttm"] == 5.0
    assert list(empty.columns) == list(mapped.columns)
    assert empty.empty


def test_capital_structure_em_adapter_maps_fields_and_symbol_suffix() -> None:
    adapter = CapitalStructureEmAdapter(symbol="600000", fetched_at=pd.Timestamp("2024-01-03 12:00:00"))
    source = pd.DataFrame(
        [
            {
                "变更日期": "2023-06-05",
                "总股本": 1268206999,
                "流通受限股份": 3620778,
                "其他内资持股(受限)": 3620778,
                "境内法人持股(受限)": 3620778,
                "境内自然人持股(受限)": None,
                "已流通股份": 1268206999,
                "已上市流通A股": 1264586221,
                "变动原因": "转增股上市",
            }
        ]
    )

    assert adapter.stock_code == "600000"
    assert adapter.akshare_symbol == "600000.SH"
    assert adapter.call(type("FakeAk", (), {"stock_zh_a_gbjg_em": staticmethod(lambda symbol: symbol)})) == "600000.SH"

    result = adapter.normalize(source)

    assert result.loc[0, "change_date"] == pd.Timestamp("2023-06-05").date()
    assert result.loc[0, "code"] == "600000"
    assert result.loc[0, "source_symbol"] == "600000.SH"
    assert result.loc[0, "total_shares"] == 1268206999
    assert result.loc[0, "change_reason"] == "转增股上市"
    assert result.loc[0, "source_endpoint"] == "stock_zh_a_gbjg_em"


def test_capital_structure_em_adapter_fetches_all_eastmoney_pages(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_get(url, params, timeout):
        calls.append(dict(params))
        assert url == "https://datacenter.eastmoney.com/securities/api/data/v1/get"
        assert timeout == 30
        page_number = int(params["pageNumber"])
        rows = [
            {
                "END_DATE": f"200{page_number}-01-01 00:00:00",
                "TOTAL_SHARES": page_number,
                "LIMITED_A_SHARES": 0,
                "LIMITED_OTHARS": None,
                "LIMITED_DOMESTIC_NOSTATE": None,
                "LIMITED_DOMESTIC_NATURAL": 0,
                "FREE_SHARES": page_number,
                "LISTED_A_SHARES": page_number,
                "CHANGE_REASON": f"reason-{page_number}",
            }
        ]
        return _FakeResponse({"success": True, "result": {"pages": 2, "count": 2, "data": rows}})

    monkeypatch.setattr("src.api.akshare.adapters.capital_structure_em.requests.get", fake_get)
    adapter = CapitalStructureEmAdapter(symbol="600000", fetched_at=pd.Timestamp("2024-01-03 12:00:00"))

    raw = adapter.call(type("FakeAk", (), {"__version__": "fake"})())
    result = adapter.normalize(raw)

    assert [call["pageNumber"] for call in calls] == ["1", "2"]
    assert all(call["pageSize"] == "500" for call in calls)
    assert calls[0]["filter"] == '(SECUCODE="600000.SH")'
    assert result["change_date"].astype(str).tolist() == ["2001-01-01", "2002-01-01"]
    assert result["total_shares"].tolist() == [1, 2]


def test_capital_structure_em_adapter_empty_frame_has_schema_columns() -> None:
    adapter = CapitalStructureEmAdapter(symbol="000001", fetched_at=pd.Timestamp("2024-01-03 12:00:00"))

    result = adapter.normalize(pd.DataFrame())

    assert list(result.columns) == [
        "change_date",
        "code",
        "source_symbol",
        "total_shares",
        "restricted_shares",
        "other_domestic_restricted_shares",
        "domestic_legal_person_restricted_shares",
        "domestic_natural_person_restricted_shares",
        "circulated_shares",
        "listed_a_shares",
        "change_reason",
        "source_endpoint",
        "fetched_at",
    ]
    assert adapter.params == {"symbol": "000001.SZ", "code": "000001"}


def test_spot_quote_eastmoney_adapter_maps_codes_volume_and_rejects_empty_data() -> None:
    adapter = SpotQuoteEastmoneyAdapter(trade_date="2024-01-03", fetched_at=datetime(2024, 1, 3, 16, 0))
    raw = pd.DataFrame(
        [
            {
                "代码": "600000",
                "名称": "PF Bank",
                "最新价": 8.3,
                "涨跌额": 0.1,
                "涨跌幅": 1.2,
                "今开": 8.2,
                "最高": 8.4,
                "最低": 8.1,
                "昨收": 8.2,
                "成交量": 10,
                "成交额": 9960.0,
                "换手率": 0.12,
                "振幅": 3.0,
                "市盈率-动态": 5.1,
                "市净率": 0.71,
                "总市值": 101000000.0,
                "流通市值": 81000000.0,
            }
        ]
    )

    mapped = adapter.normalize(raw)

    assert mapped.loc[0, "code"] == "600000"
    assert mapped.loc[0, "volume"] == 1000
    assert mapped.loc[0, "source_endpoint"] == "stock_zh_a_spot_em"
    with pytest.raises(AkShareEmptyDataError):
        adapter.normalize(pd.DataFrame())


def test_spot_quote_sina_adapter_preserves_source_symbol_and_fallback_metadata() -> None:
    adapter = SpotQuoteSinaAdapter(
        trade_date="2024-01-03",
        fallback_reason="planned",
        fetched_at=datetime(2024, 1, 3, 16, 0),
    )
    raw = pd.DataFrame(
        [
            {
                "代码": "sh600000",
                "名称": "PF Bank",
                "最新价": 8.3,
                "涨跌额": 0.1,
                "涨跌幅": 1.2,
                "买入": 8.29,
                "卖出": 8.31,
                "昨收": 8.2,
                "今开": 8.2,
                "最高": 8.4,
                "最低": 8.1,
                "成交量": 10,
                "成交额": 83.0,
                "时间戳": "15:00:00",
            }
        ]
    )

    mapped = adapter.normalize(raw)

    assert mapped.loc[0, "source_symbol"] == "sh600000"
    assert mapped.loc[0, "code"] == "600000"
    assert bool(mapped.loc[0, "is_fallback"])
    assert mapped.loc[0, "fallback_reason"] == "planned"
    with pytest.raises(AkShareEmptyDataError):
        adapter.normalize(pd.DataFrame())


def test_daily_bar_adapter_maps_adjustment_dates_volume_and_quality_status() -> None:
    adapter = DailyBarAdapter(
        symbol="000001",
        start_date="2024-01-01",
        end_date="2024-01-03",
        adjustment="unadjusted",
        fetched_at=datetime(2024, 1, 3, 16, 0),
    )
    raw = pd.DataFrame(
        [
            {
                "日期": "2024-01-03",
                "股票代码": "000001",
                "开盘": 8.2,
                "收盘": 8.3,
                "最高": 8.4,
                "最低": 8.1,
                "成交量": 10,
                "成交额": 9960.0,
                "振幅": 3.0,
                "涨跌幅": 1.2,
                "涨跌额": 0.1,
                "换手率": 0.12,
            }
        ]
    )

    mapped = adapter.normalize(raw)

    assert adapter.params["start_date"] == "20240101"
    assert adapter.params["end_date"] == "20240103"
    assert adapter.ak_adjustment == ""
    assert mapped.loc[0, "volume"] == 1000
    assert mapped.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_delist_sh_adapter_cleans_symbols_and_tracks_endpoint_metadata() -> None:
    adapter = DelistShAdapter(symbol="全部", snapshot_date="2024-01-03", fetched_at=datetime(2024, 1, 3, 16, 0))
    raw = pd.DataFrame(
        [
            {
                "公司代码": "600001",
                "公司简称": "Old Corp",
                "上市日期": "2000-01-01",
                "暂停上市日期": "2024-01-02",
            }
        ]
    )

    mapped = adapter.normalize(raw)

    assert mapped.loc[0, "code"] == "600001"
    assert mapped.loc[0, "exchange"] == "sh"
    assert mapped.loc[0, "market"] == "全部"
    assert mapped.loc[0, "source_endpoint"] == "stock_info_sh_delist"


def test_report_disclosure_adapter_maps_fields_and_empty_schema() -> None:
    adapter = ReportDisclosureAdapter(
        market="沪深京",
        period="2025年报",
        fetched_at=datetime(2026, 1, 2, 9, 0),
    )
    raw = pd.DataFrame(
        [
            {
                "股票代码": "000001",
                "股票简称": "平安银行",
                "首次预约": "2026-03-15",
                "初次变更": "",
                "二次变更": None,
                "三次变更": "2026-04-10",
                "实际披露": "2026-04-20",
            }
        ]
    )

    mapped = adapter.normalize(raw)
    empty = adapter.normalize(pd.DataFrame())

    assert adapter.endpoint == "stock_report_disclosure"
    assert adapter.params == {"market": "沪深京", "period": "2025年报"}
    assert mapped.loc[0, "report_period"] == "2025年报"
    assert str(mapped.loc[0, "period_end_date"]) == "2025-12-31"
    assert mapped.loc[0, "market"] == "沪深京"
    assert mapped.loc[0, "code"] == "000001"
    assert mapped.loc[0, "name"] == "平安银行"
    assert str(mapped.loc[0, "first_scheduled_date"]) == "2026-03-15"
    assert pd.isna(mapped.loc[0, "first_changed_date"])
    assert str(mapped.loc[0, "third_changed_date"]) == "2026-04-10"
    assert mapped.loc[0, "source_endpoint"] == "stock_report_disclosure"
    assert list(empty.columns) == list(mapped.columns)
    assert empty.empty


def test_report_disclosure_adapter_treats_akshare_empty_history_error_as_empty() -> None:
    class EmptyHistoryAkModule:
        @staticmethod
        def stock_report_disclosure(*, market: str, period: str) -> pd.DataFrame:
            raise ValueError("Length mismatch: Expected axis has 0 elements, new values have 10 elements")

    adapter = ReportDisclosureAdapter(
        market="\u6caa\u6df1\u4eac",
        period="1990\u4e00\u5b63",
        fetched_at=datetime(2026, 1, 2, 9, 0),
    )

    raw = adapter.call(EmptyHistoryAkModule())
    mapped = adapter.normalize(raw)

    assert isinstance(raw, pd.DataFrame)
    assert raw.empty
    assert mapped.empty
