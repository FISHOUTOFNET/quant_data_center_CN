"""Pending queue for capital structure refreshes triggered by Baostock factors."""

from __future__ import annotations

import os
import traceback
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from threading import RLock

import pandas as pd

from src.sources.akshare.core.symbols import normalize_akshare_code
from src.sources.akshare.pipeline.execution import AkShareUpdateRequest, update_akshare
from src.utils import paths
from src.utils.logging import logger

PENDING_FILE = Path("data/metadata/akshare_capital_structure_pending.parquet")
PENDING_COLUMNS = [
    "code",
    "trigger_dataset",
    "trigger_reason",
    "triggered_at",
    "status",
    "last_attempt_at",
    "error_stack",
]
RETRYABLE_STATUSES = {"pending", "failed_retryable"}
_PENDING_LOCK = RLock()


def enqueue_capital_structure_pending(
    root: Path | None = None,
    *,
    code: str,
    trigger_dataset: str,
    trigger_reason: str,
    now: Callable[[], datetime] | None = None,
) -> None:
    base = _root(root)
    with _PENDING_LOCK:
        stock_code = _normalize_trigger_code(code)
        current = _read_pending_unlocked(base)
        row = {
            "code": stock_code,
            "trigger_dataset": trigger_dataset,
            "trigger_reason": trigger_reason,
            "triggered_at": _timestamp_ms((now or datetime.now)()),
            "status": "pending",
            "last_attempt_at": pd.NaT,
            "error_stack": "",
        }
        active_mask = (current["code"].astype("string") == stock_code) & current["status"].isin(RETRYABLE_STATUSES)
        if active_mask.any():
            current.loc[active_mask, list(row)] = list(row.values())
            updated = current
        else:
            updated = pd.concat([current, pd.DataFrame([row])], ignore_index=True)
        _write_pending_unlocked(base, updated)


def read_capital_structure_pending(root: Path | None = None) -> pd.DataFrame:
    with _PENDING_LOCK:
        return _read_pending_unlocked(_root(root))


def drain_capital_structure_pending(
    root: Path | None = None,
    *,
    updater: Callable[[AkShareUpdateRequest], list[dict[str, object]]] = update_akshare,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, object]]:
    base = _root(root)
    with _PENDING_LOCK:
        current = _read_pending_unlocked(base)
        if current.empty:
            return []
        attempt_time = _timestamp_ms((now or datetime.now)())
        records: list[dict[str, object]] = []
        for index, row in current.loc[current["status"].isin(RETRYABLE_STATUSES)].iterrows():
            code = str(row["code"])
            try:
                result = updater(
                    AkShareUpdateRequest(
                        target="capital_structure",
                        code=(code,),
                        root=base,
                        force=True,
                        build_views=False,
                    )
                )
                if any(str(item.get("status")) == "failed" for item in result):
                    raise RuntimeError(f"capital_structure update failed for {code}: {result}")
                current.loc[index, "status"] = "success"
                current.loc[index, "error_stack"] = ""
                records.extend(result)
            except Exception as exc:
                current.loc[index, "status"] = "failed_retryable"
                current.loc[index, "error_stack"] = f"{exc}\n{traceback.format_exc()}"
                logger.warning("Capital structure pending drain failed for {}: {}", code, exc)
            current.loc[index, "last_attempt_at"] = attempt_time
        _write_pending_unlocked(base, current)
        return records


def _normalize_trigger_code(code: str) -> str:
    value = str(code).strip()
    if "." in value:
        value = value.split(".", 1)[1]
    return normalize_akshare_code(value)


def _root(root: Path | None) -> Path:
    return (root or paths.ROOT).resolve()


def _pending_path(root: Path) -> Path:
    return root / PENDING_FILE


def _empty_pending() -> pd.DataFrame:
    return pd.DataFrame(columns=PENDING_COLUMNS)


def _clean_pending(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = _empty_pending()
    for column in PENDING_COLUMNS:
        cleaned[column] = df[column] if column in df.columns else pd.NA
    cleaned["code"] = cleaned["code"].astype("string")
    cleaned["trigger_dataset"] = cleaned["trigger_dataset"].astype("string")
    cleaned["trigger_reason"] = cleaned["trigger_reason"].astype("string")
    cleaned["triggered_at"] = pd.to_datetime(cleaned["triggered_at"], errors="coerce")
    cleaned["status"] = cleaned["status"].astype("string").fillna("pending")
    cleaned["last_attempt_at"] = pd.to_datetime(cleaned["last_attempt_at"], errors="coerce")
    cleaned["error_stack"] = cleaned["error_stack"].astype("string").fillna("")
    return cleaned.reset_index(drop=True)


def _read_pending_unlocked(root: Path) -> pd.DataFrame:
    path = _pending_path(root)
    if not path.exists():
        return _empty_pending()
    try:
        return _clean_pending(pd.read_parquet(path))
    except Exception as exc:
        corrupt_path = _quarantine_corrupt_pending(path)
        logger.error("Capital structure pending queue is corrupt; quarantined {}: {}", corrupt_path, exc)
        return _empty_pending()


def _write_pending_unlocked(root: Path, df: pd.DataFrame) -> None:
    path = _pending_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f"{path.stem}.{uuid.uuid4().hex}.tmp{path.suffix}"
    try:
        _clean_pending(df).to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            with suppress(Exception):
                tmp_path.unlink()


def _quarantine_corrupt_pending(path: Path) -> Path:
    suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    corrupt_path = path.with_name(f"{path.stem}.corrupt.{suffix}{path.suffix}")
    try:
        os.replace(path, corrupt_path)
    except OSError:
        corrupt_path = path
    return corrupt_path


def _timestamp_ms(value: datetime) -> pd.Timestamp:
    return pd.Timestamp(value).floor("ms")
