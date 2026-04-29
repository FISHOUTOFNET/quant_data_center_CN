"""Local price adjustment helpers based on BaoStock adjustment factors."""

from __future__ import annotations

import pandas as pd


ADJUST_FACTOR_DATASET = "adjust_factor"
UNADJUSTED_DAILY_DATASET = "daily_k_none"
ADJUSTED_DAILY_FACTOR_COLUMNS = {
    "daily_k_qfq": "foreAdjustFactor",
    "daily_k_hfq": "backAdjustFactor",
}
PRICE_COLUMNS = ("open", "high", "low", "close", "preclose")


def is_adjusted_daily_dataset(dataset: str) -> bool:
    return dataset in ADJUSTED_DAILY_FACTOR_COLUMNS


def calculate_adjusted_daily_k(
    unadjusted: pd.DataFrame,
    adjust_factors: pd.DataFrame,
    dataset: str,
    adjustflag: str,
) -> pd.DataFrame:
    """Calculate qfq/hfq daily bars from unadjusted bars and local factors."""

    try:
        factor_column = ADJUSTED_DAILY_FACTOR_COLUMNS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unsupported adjusted daily_k dataset: {dataset}") from exc

    result = unadjusted.copy()
    if "adjustflag" in result.columns:
        result["adjustflag"] = str(adjustflag)
    if result.empty:
        return result

    result["_row_order"] = range(len(result))
    result["_date_key"] = pd.to_datetime(result["date"], errors="coerce").astype("datetime64[ns]")
    result["_code_key"] = result["code"].astype("string")
    result["_adj_factor"] = 1.0

    factor_values = _factor_values(adjust_factors, factor_column)
    if not factor_values.empty:
        for code_key, row_index in result.groupby("_code_key", dropna=False).groups.items():
            daily_dates = (
                result.loc[row_index, ["_row_order", "_date_key"]]
                .dropna(subset=["_date_key"])
                .sort_values("_date_key")
            )
            if daily_dates.empty:
                continue

            code_factors = factor_values.loc[
                factor_values["_code_key"].astype("object") == code_key,
                ["_factor_date", "_factor_value"],
            ].sort_values("_factor_date")
            if code_factors.empty:
                continue

            matched = pd.merge_asof(
                daily_dates,
                code_factors,
                left_on="_date_key",
                right_on="_factor_date",
                direction="backward",
            )
            factors = matched.set_index("_row_order")["_factor_value"].fillna(1.0)
            result.loc[factors.index, "_adj_factor"] = factors.astype(float)

    for column in PRICE_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce") * result["_adj_factor"]

    return result.drop(columns=["_row_order", "_date_key", "_code_key", "_adj_factor"])


def _factor_values(adjust_factors: pd.DataFrame, factor_column: str) -> pd.DataFrame:
    if adjust_factors.empty or factor_column not in adjust_factors.columns:
        return pd.DataFrame(columns=["_code_key", "_factor_date", "_factor_value"])

    work = adjust_factors.copy()
    work["_code_key"] = work["code"].astype("string")
    work["_factor_date"] = pd.to_datetime(work["dividOperateDate"], errors="coerce").astype("datetime64[ns]")
    work["_factor_value"] = pd.to_numeric(work[factor_column], errors="coerce")
    return work.dropna(subset=["_code_key", "_factor_date", "_factor_value"])
