"""Unit tests for the shared filesystem safety primitives.

Covers :mod:`src.utils.filesystem_safety` — the low-level mechanism that
detects link-like nodes (symlinks, Windows junctions, other reparse points)
without following them, plus the managed-root candidate authorization policy.

Cross-platform notes
--------------------
* POSIX symlink tests use REAL symlinks (created by ``Path.symlink_to``).
* Windows junction/reparse-point tests mock ``os.lstat`` and ``sys.platform``
  so they run on any OS. Real Windows junction coverage lives in
  ``tests/test_windows_junction_integration.py``.
* ``PermissionError`` / ``FileNotFoundError`` / generic ``OSError`` paths are
  mocked so they are deterministic regardless of the test interpreter.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from src.utils import filesystem_safety

# ---------------------------------------------------------------------------
# Helpers for mocking os.lstat
# ---------------------------------------------------------------------------


class _FakeStatResult:
    """Minimal stand-in for ``os.stat_result`` used by the safety helpers.

    Only the attributes read by ``is_link_like_or_reparse`` /
    ``describe_link_like`` are populated: ``st_mode`` and, on Windows,
    ``st_file_attributes`` and ``st_reparse_tag``.
    """

    def __init__(
        self,
        *,
        st_mode: int = 0o040755,  # directory by default
        st_file_attributes: int = 0,
        st_reparse_tag: int = 0,
    ) -> None:
        self.st_mode = st_mode
        self.st_file_attributes = st_file_attributes
        self.st_reparse_tag = st_reparse_tag


def _mock_lstat(monkeypatch: pytest.MonkeyPatch, path: Path, result: os.stat_result) -> None:
    """Patch ``os.lstat`` so that ``path`` returns ``result``; others fall back."""

    real_lstat = os.lstat

    def fake_lstat(target: object) -> os.stat_result:
        target_path = Path(str(target))
        if target_path == path:
            return result
        return real_lstat(target)

    monkeypatch.setattr(filesystem_safety.os, "lstat", fake_lstat)


# ---------------------------------------------------------------------------
# is_link_like_or_reparse — POSIX real symlinks
# ---------------------------------------------------------------------------


class TestIsLinkLikeOrReparsePosixSymlink:
    """Real symlink behavior on the current platform (skipped when unsupported)."""

    def test_normal_directory_is_not_link_like(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_normal_file_is_not_link_like(self, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.write_text("hi", encoding="utf-8")
        assert filesystem_safety.is_link_like_or_reparse(target) is False

    def test_real_symlink_is_link_like(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        assert filesystem_safety.is_link_like_or_reparse(link) is True

    def test_real_symlink_to_file_is_link_like(self, tmp_path: Path) -> None:
        target = tmp_path / "target.txt"
        target.write_text("hi", encoding="utf-8")
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        assert filesystem_safety.is_link_like_or_reparse(link) is True

    def test_nonexistent_path_is_not_link_like(self, tmp_path: Path) -> None:
        """``FileNotFoundError`` → returns False (no node to inspect)."""

        missing = tmp_path / "does-not-exist"
        assert filesystem_safety.is_link_like_or_reparse(missing) is False


# ---------------------------------------------------------------------------
# is_link_like_or_reparse — error paths (mocked lstat)
# ---------------------------------------------------------------------------


class TestIsLinkLikeOrReparseErrorPaths:
    """When ``lstat`` raises, ``is_link_like_or_reparse`` returns False.

    This is the low-level mechanism policy: callers that need fail-closed
    behavior must use :func:`validate_managed_root_candidate` (which raises)
    or implement their own skip-and-log policy (cleanup traversal).
    """

    def test_permission_error_returns_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_file_not_found_error_returns_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_not_found(_: object) -> os.stat_result:
            raise FileNotFoundError("missing")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_not_found)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_generic_oserror_returns_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_oserror(_: object) -> os.stat_result:
            raise OSError("boom")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_oserror)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_value_error_returns_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_value(_: object) -> os.stat_result:
            raise ValueError("bad path")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_value)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False


# ---------------------------------------------------------------------------
# is_link_like_or_reparse — Windows reparse points (mocked)
# ---------------------------------------------------------------------------


class TestIsLinkLikeOrReparseWindowsReparse:
    """Windows reparse-point detection (junctions, mount points, other tags).

    These tests mock ``sys.platform`` to ``"win32"`` and ``os.lstat`` to
    return an ``st_file_attributes`` with ``FILE_ATTRIBUTE_REPARSE_POINT`` set,
    so they run on any OS. Real junction coverage lives in the Windows
    integration test.
    """

    def test_windows_reparse_point_is_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,  # directory
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_windows_junction_tag_is_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_windows_symlink_tag_is_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_SYMLINK,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_windows_unknown_reparse_tag_is_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown reparse tags are also rejected (conservative boundary)."""

        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0xA0000099,  # unknown tag
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_windows_reparse_without_tag_attr_is_link_like(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Even when ``st_reparse_tag`` is missing, ``st_file_attributes`` alone
        is enough to identify a reparse point. The boundary rejects all of them.
        """

        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_windows_normal_directory_is_not_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_DIRECTORY,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_posix_ignores_st_file_attributes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """On POSIX, ``st_file_attributes`` is not checked even if set."""

        monkeypatch.setattr(filesystem_safety.sys, "platform", "linux")
        # Even with REPARSE_POINT attribute set, POSIX path returns False
        # because the mode is a regular directory, not a symlink.
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False


# ---------------------------------------------------------------------------
# describe_link_like
# ---------------------------------------------------------------------------


class TestDescribeLinkLike:
    """Diagnostic string for logs."""

    def test_normal_path_is_not_link_like(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        assert filesystem_safety.describe_link_like(tmp_path) == "not-link-like"

    def test_real_symlink_described_as_symlink(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        assert filesystem_safety.describe_link_like(link) == "symlink"

    def test_uninspectable_returns_uninspectable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        result = filesystem_safety.describe_link_like(tmp_path)
        assert result.startswith("uninspectable(")
        assert "PermissionError" in result

    def test_windows_junction_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "junction"

    def test_windows_symlink_tag_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_SYMLINK,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "windows-symlink"

    def test_windows_unknown_reparse_tag_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0xA0000099,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "reparse-tag=0xa0000099"

    def test_windows_reparse_without_tag_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``st_reparse_tag`` attribute is missing entirely (e.g. Python
        3.10 ``os.stat_result`` does not always populate it), the diagnostic
        falls back to ``"reparse-point"``.
        """

        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        # Remove the attribute entirely so getattr(..., None) returns None.
        del fake.st_reparse_tag
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "reparse-point"


# ---------------------------------------------------------------------------
# validate_managed_root_candidate
# ---------------------------------------------------------------------------


class TestValidateManagedRootCandidate:
    """The managed-root authorization entry used by resolve_runtime_paths."""

    def test_normal_directory_resolves(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        result = filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")
        assert result == tmp_path.resolve()

    def test_normal_file_resolves(self, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.write_text("hi", encoding="utf-8")
        result = filesystem_safety.validate_managed_root_candidate(target, source="explicit")
        assert result == target.resolve()

    def test_nonexistent_path_resolves(self, tmp_path: Path) -> None:
        """A non-existent path has no link-like node to reject; the
        repo/home/root checks in ``_reject_unsafe_log_root`` guard the
        resolved path at marker time.
        """

        missing = tmp_path / "does-not-exist"
        result = filesystem_safety.validate_managed_root_candidate(missing, source="explicit")
        assert result == missing.resolve()

    def test_real_symlink_is_rejected(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(filesystem_safety.LinkLikeError, match="symlink"):
            filesystem_safety.validate_managed_root_candidate(link, source="explicit")

    def test_lstat_permission_error_fails_closed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``lstat`` raises ``PermissionError`` for an EXISTING path,
        the helper fails closed — it does NOT silently treat the path as safe.
        """

        tmp_path.mkdir(parents=True, exist_ok=True)

        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        with pytest.raises(filesystem_safety.LinkLikeError, match="cannot inspect"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

    def test_windows_reparse_point_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        tmp_path.mkdir(parents=True, exist_ok=True)
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        with pytest.raises(filesystem_safety.LinkLikeError, match="junction"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

    def test_source_name_appears_in_error(self, tmp_path: Path) -> None:
        """The ``source`` argument is included in the error for diagnostics."""

        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(filesystem_safety.LinkLikeError, match="environment"):
            filesystem_safety.validate_managed_root_candidate(link, source="environment")

    def test_expanduser_applied_before_check(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``~`` is expanded to the home directory before the lstat check."""

        real_home = tmp_path / "fakehome"
        real_home.mkdir()
        # ``Path.expanduser`` on Windows reads ``USERPROFILE``; on POSIX it
        # reads ``HOME``. Set both so the test is cross-platform.
        monkeypatch.setenv("USERPROFILE", str(real_home))
        monkeypatch.setenv("HOME", str(real_home))
        # A real directory under the fake home should resolve normally.
        target = real_home / "logs"
        target.mkdir()
        result = filesystem_safety.validate_managed_root_candidate("~/logs", source="default")
        assert result == target.resolve()
