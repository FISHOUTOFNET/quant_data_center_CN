"""Cross-process directory locks with stale-owner recovery."""

from __future__ import annotations

import ctypes
import json
import os
import shutil
import socket
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.utils.logging import logger


class ProcessLockError(RuntimeError):
    """Raised when a lock is held by an active owner."""


@dataclass(frozen=True)
class ProcessLock:
    """A held directory lock."""

    path: Path
    owner: dict[str, object]


def is_pid_alive(pid: int) -> bool:
    """Return whether a process id appears to be alive using only the standard library."""

    if pid <= 0:
        return False
    if os.name == "nt":
        return _is_pid_alive_windows(pid)
    return _is_pid_alive_posix(pid)


@contextmanager
def acquire_process_lock(
    lock_dir: Path,
    *,
    lock_name: str,
    purpose: str,
    stale_after_seconds: int,
    extra_owner: dict[str, object] | None = None,
) -> Iterator[ProcessLock]:
    """Acquire a directory lock, replacing stale owners when it is safe to do so."""

    resolved = lock_dir.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    owner = _owner(lock_name, purpose, stale_after_seconds, extra_owner)
    while True:
        try:
            resolved.mkdir()
        except FileExistsError as exc:
            existing_owner, stale, warning = _inspect_existing_lock(resolved, stale_after_seconds)
            if warning:
                logger.warning("Treating process lock as stale: lock={} reason={}", resolved, warning)
            if not stale:
                raise ProcessLockError(f"lock is already held; lock={resolved} owner={existing_owner}") from exc
            _remove_stale_lock(resolved)
            continue
        break

    try:
        _write_owner(resolved / "owner.json", owner)
        yield ProcessLock(path=resolved, owner=owner)
    finally:
        shutil.rmtree(resolved, ignore_errors=True)


def read_lock_owner(lock_dir: Path) -> dict[str, object] | None:
    """Best-effort owner.json reader for diagnostics."""

    try:
        raw = (lock_dir / "owner.json").read_text(encoding="utf-8")
        owner = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    return owner if isinstance(owner, dict) else None


def _owner(
    lock_name: str,
    purpose: str,
    stale_after_seconds: int,
    extra_owner: dict[str, object] | None,
) -> dict[str, object]:
    owner: dict[str, object] = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "lock_name": lock_name,
        "purpose": purpose,
        "command": purpose,
        "stale_after_seconds": stale_after_seconds,
    }
    if extra_owner:
        owner.update(extra_owner)
    return owner


def _inspect_existing_lock(lock_dir: Path, stale_after_seconds: int) -> tuple[dict[str, object], bool, str | None]:
    owner = read_lock_owner(lock_dir)
    if owner is None:
        return {}, True, "owner.json missing, invalid, or corrupt"

    started_at = _parse_started_at(owner.get("started_at"))
    if started_at is None:
        return owner, True, "owner.json missing valid started_at"
    if datetime.now() - started_at > timedelta(seconds=stale_after_seconds):
        return owner, True, "owner exceeded stale_after_seconds"

    hostname = str(owner.get("hostname") or "")
    if hostname and hostname != socket.gethostname():
        return owner, False, None

    pid = _owner_pid(owner)
    if pid is None:
        return owner, True, "owner.json missing valid pid"
    if not is_pid_alive(pid):
        return owner, True, "owner pid is not alive"
    return owner, False, None


def _remove_stale_lock(lock_dir: Path) -> None:
    stale_dir = lock_dir.with_name(f"{lock_dir.name}.stale.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        lock_dir.rename(stale_dir)
    except FileNotFoundError:
        return
    except OSError:
        shutil.rmtree(lock_dir, ignore_errors=True)
        return
    shutil.rmtree(stale_dir, ignore_errors=True)


def _write_owner(path: Path, owner: dict[str, object]) -> None:
    path.write_text(json.dumps(owner, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _parse_started_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _owner_pid(owner: dict[str, Any]) -> int | None:
    try:
        pid = int(owner.get("pid"))
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _is_pid_alive_posix(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_pid_alive_windows(pid: int) -> bool:
    kernel32 = ctypes.windll.kernel32
    process_query_limited_information = 0x1000
    kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_bool, ctypes.c_ulong]
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
    kernel32.GetExitCodeProcess.restype = ctypes.c_bool
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_bool
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return False
    try:
        exit_code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == 259
    finally:
        kernel32.CloseHandle(handle)
