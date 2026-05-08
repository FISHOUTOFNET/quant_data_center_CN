from __future__ import annotations

import pandas as pd


def _fake_provider_factory(
    baostock_cn_stock_basic_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    fail_once: set[str] | None = None,
    baostock_cn_stock_adjustment_factor_df: pd.DataFrame | None = None,
):
    state = {
        "history_calls": [],
        "history_params": [],
        "baostock_cn_stock_adjustment_factor_calls": [],
        "baostock_cn_stock_adjustment_factor_params": [],
        "calendar_params": [],
        "baostock_cn_stock_basic_calls": 0,
        "fail_once": set(fail_once or set()),
    }
    factors = baostock_cn_stock_adjustment_factor_df if baostock_cn_stock_adjustment_factor_df is not None else pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": 1.0,
                "backward_adjust_factor": 1.0,
                "adjustment_factor": 1.0,
            }
        ]
    )

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
            resolved_start = start_date or "1990-01-01"
            resolved_end = end_date or "2024-12-31"
            state["calendar_params"].append({"start_date": resolved_start, "end_date": resolved_end})
            dates = pd.date_range(resolved_start, resolved_end, freq="D")
            return pd.DataFrame(
                [
                    {
                        "calendar_date": item.date(),
                        "is_trading_day": "1" if item.weekday() < 5 else "0",
                    }
                    for item in dates
                ]
            )

        def query_baostock_cn_stock_basic(self) -> pd.DataFrame:
            state["baostock_cn_stock_basic_calls"] += 1
            return baostock_cn_stock_basic_df.copy()

        def query_daily_bars(
            self,
            request,
        ) -> pd.DataFrame:
            code = request.code
            start_date = request.start_date
            end_date = request.end_date
            adjust_flag = _adjust_flag_for_dataset(request.dataset)
            state["history_calls"].append(code)
            state["history_params"].append(
                {
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "adjust_flag": adjust_flag,
                }
            )
            if code in state["fail_once"]:
                state["fail_once"].remove(code)
                raise RuntimeError(f"temporary failure for {code}")
            source = daily_df(
                code=code,
                fields=request.fields,
                start_date=start_date,
                end_date=end_date,
                frequency=request.frequency,
                adjust_flag=adjust_flag,
            ) if callable(daily_df) else daily_df
            return source.assign(code=code, adjust_flag=adjust_flag).copy()

        def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            state["baostock_cn_stock_adjustment_factor_calls"].append(code)
            state["baostock_cn_stock_adjustment_factor_params"].append(
                {
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
            source = factors(
                code=code,
                start_date=start_date,
                end_date=end_date,
            ) if callable(factors) else factors
            return source.assign(code=code).copy()

    factory = _provider_factory_for(FakeProvider)
    factory.provider_cls = FakeProvider
    return factory, state


def _provider_factory_for(provider_cls):
    def create_provider(config, provider: str | None = None):
        return provider_cls(config)

    create_provider.provider_cls = provider_cls
    return create_provider


def _adjust_flag_for_dataset(dataset: str) -> str:
    return {"baostock_cn_stock_daily_bar_unadjusted": "3", "baostock_cn_stock_daily_bar_qfq": "1", "baostock_cn_stock_daily_bar_hfq": "2"}[dataset]


def _write_settings(root, metadata_flush_size: int | None = None) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    pipeline_lines = [
        "pipeline:",
        "  lookback_days: 1",
        "  max_retries: 1",
    ]
    if metadata_flush_size is not None:
        pipeline_lines.append(f"  metadata_flush_size: {metadata_flush_size}")
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
                *pipeline_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )

