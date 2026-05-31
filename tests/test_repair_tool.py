from __future__ import annotations

import pandas as pd

import src.pipeline.repair_tool as repair_module


def test_repair_normalizes_non_trading_range_to_trading_bounds(
    tmp_path,
    monkeypatch,
    daily_sample,
) -> None:
    _write_settings(tmp_path)
    state: dict[str, list[dict[str, str]]] = {"history_params": [], "baostock_cn_stock_adjustment_factor_params": []}

    class FakeProvider:
        name = "fake"

        def __init__(self, config=None) -> None:
            self.config = config

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def query_trade_dates(
            self,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> pd.DataFrame:
            dates = pd.date_range(start_date or "1990-01-01", end_date or "2024-12-31", freq="D")
            return pd.DataFrame(
                [
                    {
                        "calendar_date": item.date(),
                        "is_trading_day": "1" if item.weekday() < 5 else "0",
                    }
                    for item in dates
                ]
            )

        def query_baostock_cn_stock_basic(
            self,
            code: str | None = None,
            code_name: str | None = None,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def query_daily_bars(
            self,
            request,
        ) -> pd.DataFrame:
            adjust_flag = {
                "baostock_cn_stock_daily_bar_unadjusted": "3",
                "baostock_cn_stock_daily_bar_qfq": "1",
                "baostock_cn_stock_daily_bar_hfq": "2",
            }[request.dataset]
            state["history_params"].append(
                {
                    "code": request.code,
                    "start_date": request.start_date,
                    "end_date": request.end_date,
                    "adjust_flag": adjust_flag,
                }
            )
            return daily_sample().assign(
                code=request.code,
                adjust_flag=adjust_flag,
            )

        def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            state["baostock_cn_stock_adjustment_factor_params"].append(
                {
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
            return pd.DataFrame(
                [
                    {
                        "code": code,
                        "dividend_operate_date": "2024-01-08",
                        "forward_adjust_factor": 1.0,
                        "backward_adjust_factor": 1.0,
                        "adjustment_factor": 1.0,
                    }
                ]
            )

    def create_provider(config, provider: str | None = None):
        return FakeProvider(config)

    monkeypatch.setattr(repair_module, "create_provider", create_provider)

    repair_module.repair(
        code="sh.600000",
        start="2024-01-06",
        end="2024-01-14",
        dataset="baostock_cn_stock_daily_bar_qfq",
        root=tmp_path,
        build_views=False,
    )

    assert state["history_params"] == [
        {
            "code": "sh.600000",
            "start_date": "2024-01-08",
            "end_date": "2024-01-12",
            "adjust_flag": "3",
        }
    ]
    assert state["baostock_cn_stock_adjustment_factor_params"] == [
        {
            "code": "sh.600000",
            "start_date": "1990-01-01",
            "end_date": "2024-01-12",
        }
    ]


def _write_settings(root) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "api:",
                "  baostock:",
                "    adjust_flag_map:",
                '      unadjusted: "3"',
                '      qfq: "1"',
                '      hfq: "2"',
                "datasets:",
                "  daily_bar:",
                '    fields: "date,code,open,high,low,close,prev_close,volume,amount,adjust_flag,turn,trade_status,pct_change,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,is_st"',
                "    frequency: d",
                "pipeline:",
                "  max_retries: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
