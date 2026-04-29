from __future__ import annotations

import pandas as pd


def _fake_provider_factory(
    stock_basic_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    fail_once: set[str] | None = None,
    adjust_factor_df: pd.DataFrame | None = None,
):
    state = {
        "history_calls": [],
        "history_params": [],
        "adjust_factor_calls": [],
        "adjust_factor_params": [],
        "calendar_params": [],
        "stock_basic_calls": 0,
        "fail_once": set(fail_once or set()),
    }
    factors = adjust_factor_df if adjust_factor_df is not None else pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividOperateDate": "2024-01-02",
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 1.0,
                "adjustFactor": 1.0,
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

        def query_stock_basic(self) -> pd.DataFrame:
            state["stock_basic_calls"] += 1
            return stock_basic_df.copy()

        def query_daily_k(
            self,
            request,
        ) -> pd.DataFrame:
            code = request.code
            start_date = request.start_date
            end_date = request.end_date
            adjustflag = _adjustflag_for_dataset(request.dataset)
            state["history_calls"].append(code)
            state["history_params"].append(
                {
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "adjustflag": adjustflag,
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
                adjustflag=adjustflag,
            ) if callable(daily_df) else daily_df
            return source.assign(code=code, adjustflag=adjustflag).copy()

        def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            state["adjust_factor_calls"].append(code)
            state["adjust_factor_params"].append(
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


def _adjustflag_for_dataset(dataset: str) -> str:
    return {"daily_k_none": "3", "daily_k_qfq": "2", "daily_k_hfq": "1"}[dataset]


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
                "    adjustflag_map:",
                '      none: "3"',
                '      qfq: "2"',
                '      hfq: "1"',
                "datasets:",
                "  daily_k:",
                '    fields: "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"',
                "    frequency: d",
                *pipeline_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )
