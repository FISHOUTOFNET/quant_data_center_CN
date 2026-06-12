from __future__ import annotations

import json
import socket
from datetime import datetime
from pathlib import Path

import pytest

from src.utils.process_lock import ProcessLockError, acquire_process_lock


def test_process_lock_acquire_release_removes_lock_dir(tmp_path: Path) -> None:
    lock_dir = tmp_path / "sample.lock"

    with acquire_process_lock(
        lock_dir,
        lock_name="sample",
        purpose="test",
        stale_after_seconds=60,
    ) as lock:
        assert lock.path == lock_dir
        assert (lock_dir / "owner.json").is_file()

    assert not lock_dir.exists()


def test_process_lock_rejects_active_owner(tmp_path: Path) -> None:
    lock_dir = tmp_path / "sample.lock"

    with acquire_process_lock(lock_dir, lock_name="sample", purpose="outer", stale_after_seconds=60):
        with pytest.raises(ProcessLockError, match="owner="):
            with acquire_process_lock(lock_dir, lock_name="sample", purpose="inner", stale_after_seconds=60):
                pass


def test_process_lock_recovers_dead_pid_owner(tmp_path: Path) -> None:
    lock_dir = tmp_path / "sample.lock"
    lock_dir.mkdir()
    (lock_dir / "owner.json").write_text(
        json.dumps(
            {
                "pid": 99999999,
                "hostname": socket.gethostname(),
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "lock_name": "sample",
                "purpose": "test",
                "stale_after_seconds": 60,
            }
        ),
        encoding="utf-8",
    )

    with acquire_process_lock(lock_dir, lock_name="sample", purpose="new", stale_after_seconds=60):
        assert (lock_dir / "owner.json").is_file()

    assert not lock_dir.exists()


def test_process_lock_recovers_corrupt_owner(tmp_path: Path) -> None:
    lock_dir = tmp_path / "sample.lock"
    lock_dir.mkdir()
    (lock_dir / "owner.json").write_text("{bad", encoding="utf-8")

    with acquire_process_lock(lock_dir, lock_name="sample", purpose="new", stale_after_seconds=60):
        owner = json.loads((lock_dir / "owner.json").read_text(encoding="utf-8"))
        assert owner["purpose"] == "new"
